package cn._51doit.flink.day07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

//累加当前串口的数据，并与历史数据进行累加
public class WindowAggregateFunctionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(10000);
        //1000,spark,3
        //1200,spark,5
        //2000,hadoop,2
        //socketTextStream返回的DataStream并行度为1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> dataWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, timestamp) -> Long.parseLong(line.split(",")[0])));


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = dataWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        //调用keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);

        OutputTag<Tuple2<String, Integer>> lateDataTag = new OutputTag<Tuple2<String, Integer>>("late-data") {
        };

        //NonKeyd Window： 不调用KeyBy，然后调用windowAll方法，传入windowAssinger
        // Keyd Window： 先调用KeyBy，然后调用window方法，传入windowAssinger
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed = keyed
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //如果直接调用sum或reduce，只会聚合窗口内的数据，不去跟历史数据进行累加
        //需求：可以在窗口内进行增量聚合，并且还可以与历史数据进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowed.aggregate(new MyAggFunc(), new MyWindowFunc());

        result.print();

        env.execute();

    }


    private static class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        //创建一个初始值
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        //数据一条数据，与初始值或中间累加的结果进行聚合
        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return value.f1 + accumulator;
        }

        //返回的结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        //如果使用的是非SessionWindow，可以不实现
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


    private static class MyWindowFunc extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> sumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-state", Integer.class);
            sumState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

            Integer historyCount = sumState.value();
            if (historyCount == null) {
                historyCount = 0;
            }
            //获取到窗口聚合后输出的结果
            Integer windowCount = elements.iterator().next();
            Integer totalCount = historyCount + windowCount;
            //更新状态
            sumState.update(totalCount);

            //输出
            out.collect(Tuple2.of(key, totalCount));
        }
    }
}
