package com.zzml.flinklearn.doitedu;

import com.google.common.base.Charsets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
import java.util.List;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:SourceOperatorDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/10 09:26
 * @Version: v1.0
 * @ModifyDate:
 */

public class SourceOperatorDemo {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String topic = parameterTool.get("kafka.topic");
        final String groupId = parameterTool.get("kafka.group.id");
        final String latest = parameterTool.get("kafka.offset", "latest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("hadoop100", 8888);

        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4, 5);

        numDS.map(num -> num * 10);

        List<String> dataList = Arrays.asList("a", "b", "a", "c");
        DataStreamSource<String> fromCollection = env.fromCollection(dataList);

        fromCollection.map(String::toUpperCase);

        // fromParallelCollection所返回的source算子是一个多并行的source算子
        DataStreamSource<LongValue> longValueDataStreamSource = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class)).setParallelism(2);
        longValueDataStreamSource.map(lv -> lv.getValue() + 100);

        /**
         * 引入扩展包 ：  flink-connector-kafka
         * 从kafka中读取数据得到数据流
         * 新版本写法
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // 设置订阅的topic和消费组id,以及服务器
                .setTopics(topic)
                .setGroupId(groupId)
                .setBootstrapServers("hadoop100:9092,hadoop101:9092")

                /**
                 * 指定消费偏移量
                 * OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 消费起始位移选择之前所提交的偏移量（如果没有，则重置为LATEST）
                 * OffsetsInitializer.earliest()  消费起始位移直接选择为 “最早”
                 * OffsetsInitializer.latest()  消费起始位移直接选择为 “最新”
                 * OffsetsInitializer.offsets(Map<TopicPartition,Long>)  消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                 */
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 设置value数据的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                /**
                 * "auto.offset.commit", "true"
                 * 开启kafka底层消费者的自动位移提交机制
                 *    它会把最新的消费位移提交到kafka的consumer_offsets中
                 *    就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                 *    （宕机重启时，优先从flink自己的状态中去获取偏移量<更可靠>）
                 */
                .setProperty("auto.offset.commit", "true")

                /**
                 * 把本source算子设置成  BOUNDED属性（有界流）
                 *     将来本source去读取数据的时候，读到指定的位置，就停止读取并退出
                 *     常用于补数或者重跑某一段历史数据
                 */
//                .setBounded(OffsetsInitializer.committedOffsets())

                /**
                 * 把本source算子设置成  UNBOUNDED属性（无界流）
                 *     但是并不会一直读数据，而是达到指定位置就停止读取，但程序不退出
                 *     主要应用场景：需要从kafka中读取某一段固定长度的数据，然后拿着这段数据去跟另外一个真正的无界流联合处理
                 */
//                .setUnbounded(OffsetsInitializer.latest())

                .build();

        // env.addSource();  //  接收的是  SourceFunction接口的 实现类
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");//  接收的是 Source 接口的实现类
        streamSource.print();


        env.execute();


    }

}


