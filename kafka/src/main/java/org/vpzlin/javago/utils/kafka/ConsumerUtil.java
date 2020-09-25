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

    public ConsumerUtil(String ipAndPorts, String topic, String groupId){
        setConsumer(ipAndPorts, topic, groupId);
    }

    /**
     * 设置消费者
     * @param hostAndPorts 主机和端口集，格式如： server1:9092,server2:9092
     * @param topic 主题
     * @param groupId 消费者组ID
     */
    public void setConsumer(String hostAndPorts, String topic, String groupId){
        Properties props = new Properties();
        props.put("bootstrap.servers", hostAndPorts);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<String, String>(props);
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    /**
     * 获取Kafka消费者
     * @return Kafka消费者
     */
    public KafkaConsumer<String, String> getKafkaConsumer(){
        return this.kafkaConsumer;
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
