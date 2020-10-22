package org.vpzlin.javago.utils.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamUtil {
    Properties properties;
    final StreamsBuilder builder = new StreamsBuilder();

    /**
     * 构造函数（有默认stream-app名）
     * @param kafkaHostAndPorts Kafka服务器与端口，格式如： server1:9092,server2:9092
     */
    public StreamUtil(String kafkaHostAndPorts){
        properties = getKafkaServerProperties(kafkaHostAndPorts, "KafkaStream-App");
    }

    /**
     * 构造函数
     * @param kafkaHostAndPorts Kafka服务器与端口，格式如： server1:9092,server2:9092
     * @param appId stream-app名
     */
    public StreamUtil(String kafkaHostAndPorts, String appId){
        properties = getKafkaServerProperties(kafkaHostAndPorts, appId);
    }

    /**
     * 获取Kafka服务器属性
     * @param kafkaHostAndPorts Kafka服务器与端口，格式如： server1:9092,server2:9092
     * @return
     */
    private Properties getKafkaServerProperties(String kafkaHostAndPorts, String appId){
        Properties properties = new Properties();
        // Stream唯一标识
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostAndPorts);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        return properties;
    }

    /**
     * 同步主题
     * @param topicNameSource 源主题名
     * @param topicNameTarget 目标主题名
     */
    public void syncTopic(String topicNameSource, String topicNameTarget){
        builder.stream(topicNameSource).to(topicNameTarget);
        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(10);

        try{
            kafkaStreams.start();
            latch.await();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            kafkaStreams.close();
            latch.countDown();
        }
    }

    /**
     * Word Count
     */
    public void wordCount(String topicNameSource, String splitWord, String topicNameTarget){
        KStream<String, String> source = builder.stream(topicNameSource);
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(splitWord)))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
                .to(topicNameTarget, Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        final CountDownLatch latch = new CountDownLatch(1);

        // 开始流处理
        try{
            kafkaStreams.start();
            latch.await();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            kafkaStreams.close();
            latch.countDown();
        }
    }

    public static void main(String[] args){
        String kafkaHostAndPorts = "192.168.108.110:9092";
        String appId = "KafkaStream-app";
        StreamUtil streamUtil = new StreamUtil(kafkaHostAndPorts, appId);

        /* 同步主题 */
        streamUtil.syncTopic("test_topic", "test_topic_pipe");

        /* WordCount */
        String splitWord = "\\W+";
        streamUtil.wordCount("test_topic_producer", splitWord, "test_topic_wordcount");
    }
}
