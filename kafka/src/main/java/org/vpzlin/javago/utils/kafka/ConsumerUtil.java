package org.vpzlin.javago.utils.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ConsumerUtil {
    KafkaConsumer<String, String> kafkaConsumer;

    public ConsumerUtil(String kafkaHostAndPorts, String topicName, String groupId){
        this.kafkaConsumer = new KafkaConsumer<String, String>(getKafkaServerProperties(kafkaHostAndPorts, groupId));
        this.kafkaConsumer.subscribe(Arrays.asList(topicName));
    }

    /**
     * 获取Kafka服务器属性
     * @param kafkaHostAndPorts Kafka服务器与端口，格式如： server1:9092,server2:9092
     * @param groupId 消费组ID，一个topic的数据可被多个消费组消费，消费组之间相互独立，一个消费组可以含有多个消费者
     * @return Kafka服务器属性
     */
    public Properties getKafkaServerProperties(String kafkaHostAndPorts, String groupId){
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaHostAndPorts);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        return properties;
    }

    /**
     * 关闭Kafka消费者
     */
    public void closeKafkaConsumer(){
        this.kafkaConsumer.close();
    }

    /**
     * 消费：获取消息
     * @return
     */
    public LinkedHashMap<String, String> poll(){
        LinkedHashMap<String, String> data = new LinkedHashMap<>(8);
        for(ConsumerRecord<String, String> record: kafkaConsumer.poll(Duration.ofSeconds(1))){
            data.put(record.key(), record.value());
        }

        return data;
    }

    public static void main(String[] args){
        // 主题
        String topicName = "test_topic_producer";

        topicName = "KafkaStream-app-counts-store-changelog";
        // 消费组
        String groupId = "group_consumer";
        // 初始化消费者
        ConsumerUtil consumerUtil = new ConsumerUtil("192.168.108.110:9092", topicName, groupId);
        // 消费topic
        while (true){
            LinkedHashMap<String, String> data = consumerUtil.poll();
            for(Map.Entry<String, String> entry: data.entrySet()){
                System.out.printf("Got message: key [%s], value [%s]\n", entry.getKey(), entry.getValue());
            }
        }
    }
}
