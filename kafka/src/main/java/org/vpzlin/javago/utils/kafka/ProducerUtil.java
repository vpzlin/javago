package org.vpzlin.javago.utils.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.admin.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class ProducerUtil {
    Properties properties;
    AdminClient adminClient;
    KafkaProducer<String, String> kafkaProducer;

    public ProducerUtil(String kafkaHostAndPorts){
        this.properties = getKafkaServerProperties(kafkaHostAndPorts);
        this.adminClient = AdminClient.create(properties);
        this.kafkaProducer = new KafkaProducer<>(this.properties);
    }

    /**
     * 获取Kafka服务器属性
     * @param kafkaHostAndPorts Kafka服务器与端口，格式如： server1:9092,server2:9092
     * @return Kafka服务器属性
     */
    public Properties getKafkaServerProperties(String kafkaHostAndPorts){
        properties = new Properties();
        /* 设置服务器与端口 */
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaHostAndPorts);

        /* 设置必须要有多少个分区副本收到消息，才会认为写入是成功的，官方文档中的默认值为all：
               acks=0    写入不等待任何响应，吞吐量最高，但丢数据无从而知。
               acks=1    写入会等待首领节点的响应，否则会重发消息。如果原首领节点挂掉，而新选举的首领之前未收到数据，也会造成数据丢失。吞吐量方面，采用异步发送的方式会高一些。
               acks=all  所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应。
         */
        properties.put("acks", "all");

        /* 设置重试次数，官方文档中无默认值 */
        properties.put("retries", 1);

        /* 设置生产者在发送批次之前等待更多消息加入批次的时间（单位：毫秒），官方文档中的默认值为1。
         */
        properties.put("linger.ms", 1);

        /* 每次提交消息批次的内存大小，该空间被填满时会自动提交，但未填满时也可能被发送。该值设得太小会导致频繁发送消息，从而增加一些额外开销，官方文档中的默认值为16384。
         */
        properties.put("batch.size", 16384);

        /*  设置生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的信息，官方文档中的默认值为33554432。
            如果应用发送消息的速度超过发送到服务器的速度，会导致生产者空间不足，这时send()函数调用会被阻塞或者抛出异常。
         */
        properties.put("buffer.memory", 33554432);

        /* 设置key的序列化器，支持的类型有，官方文档中的默认值为StringSerializer：
                 ByteArraySerializer   （只做很少的事情）
                 StringSerializer
                 IntegerSerializer
         */
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /* value的序列化器与key的参数类型一致 */
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    /**
     * 生产：发送消息
     * @param topicName 主题
     * @param key 键
     * @param value 值
     */
    public void send(String topicName, String key, String value){
        this.kafkaProducer.send(new ProducerRecord<>(topicName, key, value));
    }

    /**
     * 判断主题是否存在
     * @param topicName 主题名
     * @return 存在返回true，不存在或失败返回false
     */
    public boolean existTopic(String topicName){
        Collection<String> topicNames = new ArrayList<>();
        topicNames.add(topicName);

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
        try {
            describeTopicsResult.all().get();
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 创建主题
     * @param topicName 主题名
     * @param numPartitions 分区数
     * @param numReplica 副本数（副本数不能超过Kafka节点数，如Kafka有3台，则该值最大为3）
     * @return 成功返回true，失败返回false
     */
    public boolean createTopic(String topicName, int numPartitions, short numReplica){
        NewTopic newTopic = new NewTopic(topicName, numPartitions, numReplica);
        Collection<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);

        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
        try {
            createTopicsResult.all().get();
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 删除主题
     * @param topicName 主题名
     * @return 成功返回true，失败返回false
     */
    public boolean deleteTopic(String topicName){
        Collection<String> topicNames = new ArrayList<>();
        topicNames.add(topicName);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicNames);
        try {
            deleteTopicsResult.all().get();
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 生产：发送消息
     * @param topicName 主题
     * @param partition 分区
     * @param key 键
     * @param value 值
     */
    public void send(String topicName, int partition, String key, String value){
        this.kafkaProducer.send(new ProducerRecord<>(topicName, partition, key, value));
    }

    /**
     * 立即发送缓存中的消息
     */
    public void flush(){
        this.kafkaProducer.flush();
    }

    /**
     * 关闭Kafka生产者
     */
    public void closeKafkaProducer(){
        this.kafkaProducer.close();
    }

    /**
     * 逐步调试
     */
    public static void testStep(){
        // Kafka主题名
        String topicName = "test_topic";
        // Kafka服务器与端口，多台服务器的配置规则参照"server1:9092,server2:9092,server3:9092"
        String kafkaHostAndPorts = "192.168.108.110:9092";

        // 初始化生产者类
        ProducerUtil producerUtil = new ProducerUtil(kafkaHostAndPorts);

        /* 创建主题 */
        // 分区数
        int numPartitions = 3;
        // 副本数（副本数不能超过Kafka节点数，如Kafka有3台，则该值最大为3）
        short numReplica = 1;
        // 创建主题
        System.out.printf("主题[%s]是否创建成功：%s\n", topicName, producerUtil.createTopic(topicName, numPartitions, numReplica));

        /* 存在主题 */
        System.out.printf("主题[%s]是否存在：%s\n", topicName, producerUtil.existTopic(topicName));

        /* 删除主题 */
        System.out.printf("主题[%s]是否删除成功：%s\n", topicName, producerUtil.deleteTopic(topicName));

        /* 发送消息 */
        // 不指定消息的分区
        producerUtil.send(topicName, "发送的key", "发送的值");
        // 指定消息的分区为第0个分区
        producerUtil.send(topicName, 0, "发送的key", "发送的值");

        /* 关闭Kafka生产者 */
        producerUtil.closeKafkaProducer();
    }

    public static void test01() throws InterruptedException {
        String topicName = "test_topic_producer";
        String kafkaHostAndPorts = "192.168.108.110:9092";
        ProducerUtil producerUtil = new ProducerUtil(kafkaHostAndPorts);
        for(int i = 0; ; i++){
            producerUtil.send(topicName, String.valueOf(i), String.format("%s %s %s", i, i*i, i*i*i));
            System.out.println("send: key=[" + i + "], value=[" + String.format("%s %s %s", i, i*i, i*i*i) + "]");
            Thread.sleep(10);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 逐步调试
//        testStep();

        test01();
    }
}
