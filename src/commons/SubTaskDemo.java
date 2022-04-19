package cn._51doit.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Task的划分和subTask的数量问题
 */
public class SubTaskDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2); //设置并行度为2
        //设置Kafka相关参数
        Properties properties = new Properties();//设置Kafka的地址和端口
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-1.51doit.cn:9092,node-1.51doit.cn:9092");
        //读取偏移量策略：如果没有记录偏移量，就从头读，如果记录过偏移量，就接着读
        properties.setProperty("auto.offset.reset", "earliest");
        //设置消费者组ID
        properties.setProperty("group.id", "g1");
        //没有开启checkpoint，让flink提交偏移量的消费者定期自动提交偏移量
        properties.setProperty("enable.auto.commit", "true");
        //创建FlinkKafkaConsumer并传入相关参数
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "wm18", //要读取数据的Topic名称
                new SimpleStringSchema(), //读取文件的反序列化Schema
                properties //传入Kafka的参数
        );
        //使用addSource添加kafkaConsumer

        //1000,spark,3
        //Source的并行度？ 2
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        //(spark, 3)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        //先调用KeyBy在划分window
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndCount.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (Tuple2<String, Integer> tp : input) {
                            out.collect(tp);
                        }

                    }
                });

        //调用printSink
        result.print().setParallelism(1);

        env.execute();

    }
}
