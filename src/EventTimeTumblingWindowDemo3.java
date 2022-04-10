package cn._51doit.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

//先keyBy，再划分EventTime的划分滚动窗口
//KafkaSource，分区为3，StreamExecutionEnvironment的默认并行为4
//不在Source对应的DataStream生成watermark，而是先调用map生成一些新的DataStream，在这个新的DataStream上生成watermark
public class EventTimeTumblingWindowDemo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

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
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> dataWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = dataWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        //调用keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);

        //NonKeyd Window： 不调用KeyBy，然后调用windowAll方法，传入windowAssinger
        // Keyd Window： 先调用KeyBy，然后调用window方法，传入windowAssinger
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        windowed.sum(1).print();

        env.execute();

    }
}
