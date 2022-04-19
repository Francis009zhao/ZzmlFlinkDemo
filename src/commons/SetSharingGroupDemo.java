package cn._51doit.flink.day05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SetSharingGroupDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //并行度就是1
        DataStreamSource<String> lines = env.socketTextStream(args[0], 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<String> filtered = words.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.startsWith("error");
            }
        }).setParallelism(2).disableChaining().slotSharingGroup("doit");

        //从这个算子开始，后期的task的共享资源槽的名称都是doit（就近原则）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).setParallelism(2).slotSharingGroup("default");

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(t -> t.f0).sum(1).setParallelism(2).slotSharingGroup("default");;

        summed.print().setParallelism(2).slotSharingGroup("default");;

        env.execute();


    }
}
