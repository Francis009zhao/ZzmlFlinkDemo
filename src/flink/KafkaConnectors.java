package com.zzml.flink.connectors;


import com.zzml.flink.commons.FlinkProjectConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;


import java.util.Properties;

/**
 * DEC: 定义kafka连接器
 * Date: 2022-03-12
 * Author: zzml
 */
public class KafkaConnectors {

    /**
     * DEC：传入topic，即可以将数据输出到kafka，String类型，一般用于侧输出数据的输出，方便操作
     *
     * @param topic 默认： side-output-data-stream-all
     * @return kafka生产者连接器
     */

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(FlinkProjectConstant.DEFAULT_BROKERS,
                topic,
                new SimpleStringSchema());
    }

    /**
     * 传入fastjson的系列化类，将数据以json格式输出到kafka
     *
     * @param kafkaSerializationSchema 传入序列化类
     * @param <T>   : JSONObject类型 fastjson
     * @return FlinkKafkaProducer
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        Properties properties = new Properties();
        properties.setProperty(FlinkProjectConstant.KAFKA_BOOTSTRAP_SERVERS, FlinkProjectConstant.DEFAULT_BROKERS);
        /**
         *  开启ck需要设置此配置，否则会报事务的超时时间过小异常：第一个数据来了开始预提交事务，当所有数据都到了，ck完成后做真正的提交，
         *  kafka的超时时间>ck的超时时间。   若ck>kafka:kafka先挂掉，ck成功，数据读取不一致；若ck<kafka：ck失败，kafka必失败。
         *  待补充相关知识点：20220523
         */
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000 + "");

        return new FlinkKafkaProducer<T>(FlinkProjectConstant.DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     *  TODO： 定义新版的sink到kafka的方法，暂没实现
     *
     public static <T> KafkaSink<T> getKafkaSink(String topic, KafkaSerializationSchema<T> kafkaSerializationSchema, DeliveryGuarantee deliveryGuarantee) {
     return KafkaSink.<T>builder()
     .setBootstrapServers(FlinkProjectConstant.DEFAULT_BROKERS)
     .setRecordSerializer(KafkaRecordSerializationSchema.builder()
     .setTopic(topic)
     .setValueSerializationSchema(new )
     .build())
     .setDeliverGuarantee(deliveryGuarantee)
     .build();
     }
     */


}
