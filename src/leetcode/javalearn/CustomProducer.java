package com.zzml.flinklearn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:CustomProducer
 * @Auther: zzml
 * @Description: kafka生产者
 * @Date: 2022/7/3 21:27
 * @Version: v1.0
 * @ModifyDate:
 */

public class CustomProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop100:9092,hadoop101:9092");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //  2.发送数据
        for (int i = 0; i < 30; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "atguigu" + i));
        }

        // 3.关闭资源
        kafkaProducer.close();

    }

}
