package org.vpzlin.javago.utils.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerUtil {
    KafkaConsumer<String, String> kafkaConsumer;

    public ConsumerUtil(String kafkaHostAndPorts, String topicName, String groupId){
        this.kafkaConsumer = new KafkaConsumer<String, String>(getKafkaServerProperties(kafkaHostAndPorts, groupId));
        this.kafkaConsumer.subscribe(Arrays.asList(topicName));
    }

    public Properties getKafkaServerProperties(String kafkaHostAndPorts, String groupId){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaHostAndPorts);
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
    public Map<String, String> poll(){
        HashMap<String, String> data = new HashMap<>(8);
        for(ConsumerRecord<String, String> record: kafkaConsumer.poll(Duration.ofSeconds(1))){
            data.put(record.key(), record.value());
        }

        return data;
    }

    public static void main(String[] args){
        String topic = "test_topic";
        String groupId = "test_group";
        ConsumerUtil consumerUtil = new ConsumerUtil("192.168.108.110:9092", topic, groupId);
        while (true){
            Map<String, String> data = consumerUtil.poll();
            for(Map.Entry<String, String> entry: data.entrySet()){
                System.out.printf("Got message: key [%s], value [%s]\n", entry.getKey(), entry.getValue());
            }
        }
    }
}
