package org.vpzlin.javago.utils.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerUtil {
    KafkaProducer<String, String> kafkaProducer;

    public ProducerUtil(String ipAndPorts){
        setKafkaProducer(ipAndPorts);
    }

    /**
     * 设置生产者
     * @param hostAndPorts 主机和端口集，格式如： server1:9092,server2:9092
     */
    public void setKafkaProducer(String hostAndPorts){
        Properties props = new Properties();
        props.put("bootstrap.servers", hostAndPorts);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);

        /*
           设置key的序列化器，支持的类型有：
                 ByteArraySerializer   （只做很少的事情）
                 StringSerializer      （推荐默认）
                 IntegerSerializer
         */
        props.put("key.serializer", StringSerializer.class.getName());
        // value的序列化器与key的参数类型一致
        props.put("value.serializer", StringSerializer.class.getName());

        // 初始Kafka化生产者
        this.kafkaProducer = new KafkaProducer<String, String>(props);
    }

    /**
     * 获取Kafka生产者
     * @return
     */
    public KafkaProducer<String, String> getKafkaProducer(){
        return this.kafkaProducer;
    }

    /**
     * 生产：发送消息
     * @param topic 主题
     * @param key 键
     * @param value 值
     */
    public void send(String topic, String key, String value){
        this.kafkaProducer.send(new ProducerRecord<>(topic, key, value));
    }

    /**
     * 关闭Kafka生产者
     */
    public void closeKafkaProducer(){
        this.kafkaProducer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "test_topic";
        ProducerUtil producerUtil = new ProducerUtil("192.168.108.110:9092");
        for (int i = 0; i >= 0; i++){
            producerUtil.send(topic, "test key " + i, "test value " + i);
            System.out.printf("Send message: topic [%s], key [%s], value [%s]\n", topic, i, i);
            Thread.sleep(1000);
        }
        producerUtil.closeKafkaProducer();
    }
}
