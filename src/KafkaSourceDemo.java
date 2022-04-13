package com.zzml.flinklearn.doit.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置kafka相关参数
        Properties properties = new Properties();
        //设置kafka的地址和端口
        properties.setProperty("bootstrap.servers", "node01:9092, node02:9092, node03:9092");
        //读取偏移量策略：如果没有记录偏移量，从头读，如果记录过偏移量，就接着读
        properties.setProperty("auto.offset.reset", "earliest");
        //设置消费组,偏移量跟group.id+topic+分区来记录的
        properties.setProperty("group.id", "group_id_ods");

        /**
         * 没有开启checkpoint，让flink提交偏移量的消费者定期自动提交偏移量，这个任务有jobManager负责处理
         * 下面记录偏移量的方式，不可靠，可能存在数据消费完成后，但任务没有记录偏移量，所以我们可以将偏移量存储到
         * hdfs上的stateBackEnd，如果任务重启，可以通过外部系统读取偏移量来恢复数据。
         */

        properties.setProperty("enable.auto.commit", "true");

        //创建kafka，并传入相关参数
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "ods-word",
                new SimpleStringSchema(),   //读取文件的反序列化schema，此处可以自定义反序列化。
                properties
        );


        // addSource
        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);

        // transformation
        SingleOutputStreamOperator<String> wordsDS = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //lambda写法
//        SingleOutputStreamOperator<String> wordsDataStream = dataStreamSource.flatMap((String lines, Collector<String> out) -> Arrays.stream(lines.split(","))
//                .forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsAndOne = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordsAndOne.keyBy(0).sum(1);

        summed.print();

        env.execute();

    }

}
