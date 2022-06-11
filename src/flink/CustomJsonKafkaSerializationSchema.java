package com.zzml.flink.commons;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:CustomJsonKafkaSerializationSchema
 * @Auther: zzml
 * @Description: 自定义的json格式的kafka序列化器，fastjson，而不是flink提供的json
 *               CustomJsonKafkaSerializationSchema 实现 KafkaSerializationSchema，将数据序列化成json格式，
 *               通过调用该类，将json格式的数据输出到kafka中。
 * @Date: 2022/5/22 22:07
 * @Version: v1.0
 * @ModifyDate:
 */

public class CustomJsonKafkaSerializationSchema implements KafkaSerializationSchema<JSONObject> {

    private static final long serialVersionUID = 1L;

    private String topic;

    // 通过构造方法topic传进来
    public CustomJsonKafkaSerializationSchema(String topic){
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic,element.toJSONString().getBytes());
    }
}
