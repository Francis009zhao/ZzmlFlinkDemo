package cn._51doit.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class KeyedProcessFunctionDemo {

    public static void main(String[] args) throws Exception{

        //创建Flink流计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //创建DataStream
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //调用Transformation开始
        //调用Transformation
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpDataStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpDataStream.keyBy(t -> t.f0);

        //对KeyedDataStream调用process方法，可以获取KeyedState
        SingleOutputStreamOperator<Tuple3<String, String, Double>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {

            private transient MapState<String, Double> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义一个状态描述器
                MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<String, Double>("kv-state", String.class, Double.class);
                //初始化或恢复历史状态
                mapState = getRuntimeContext().getMapState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String city = value.f1;
                Double money = value.f2;
                Double historyMoney = mapState.get(city);
                if (historyMoney == null) {
                    historyMoney = 0.0;
                }
                Double totalMoney = historyMoney + money; //累加
                //更新到state中
                mapState.put(city, totalMoney);
                //输出
                value.f2 = totalMoney;
                out.collect(value);
            }
        });

        result.print();

        //启动执行
        env.execute("StreamingWordCount");

    }
}
