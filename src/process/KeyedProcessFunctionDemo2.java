package cn._51doit.flink.day07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 *
 */
public class KeyedProcessFunctionDemo2 {

    public static void main(String[] args) throws Exception{

        //创建Flink流计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //创建DataStream
        //Source
        //1000,spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> dataStreamWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO) //乱序延迟的实际
                .withTimestampAssigner((line, timestamp) -> Long.parseLong(line.split(",")[0]))); //提取eventTime生成waterMark


        //调用Transformation
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpDataStream = dataStreamWithWaterMark.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = tpDataStream.keyBy(t -> t.f1);

        //对KeyedDataStream调用process方法，可以获取KeyedState
        //数据按照eventTime一分钟触发一次（不划分窗口，而是使用注册定时器的方式）
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple3<Long, String, Integer>, Tuple2<String, Integer>>() {

            private transient ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("lst-state", Integer.class);
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple3<Long, String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                listState.add(value.f2);
                long currentWatermark = ctx.timerService().currentWatermark();
                long fireTime = currentWatermark - currentWatermark % 60000 + 60000;
                ctx.timerService().registerEventTimeTimer(fireTime);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                Iterable<Integer> it = listState.get();
                ArrayList<Integer> nums = new ArrayList<>();
                for (Integer i : it) {
                    nums.add(i);
                }
                nums.sort((a, b) -> b - a);
                //只输出topN（n = 3）
                int loop = Math.min(nums.size(), 3);
                for (int i = 0; i < loop; i++) {
                    out.collect(Tuple2.of(ctx.getCurrentKey(), nums.get(i)));
                }
                listState.clear();
            }
        });

        result.print();

        //启动执行
        env.execute("StreamingWordCount");

    }
}
