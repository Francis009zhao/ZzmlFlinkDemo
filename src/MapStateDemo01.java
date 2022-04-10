package com.zzml.flinklearn.doit.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyBy之后，用来存储K-V类型的状态，叫做KeyedState
 * <p>
 * KeyedState种类有：ValueState<T> （value是一个基本类型、集合类型、自定义类型）
 * MapState<小K,V> （存储的是k，v类型）  （大K -> (小K， 小V)）
 * ListState（Value是一个list集合）
 */
public class MapStateDemo01 {

    public static void main(String[] args) throws Exception {

        //创建Flink流计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //创建DataStream
        //Source
        DataStreamSource<String> lines = env.socketTextStream("192.168.68.100", 8888);

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

        /**
         * 也可以通过字符串拼接，将keyBy粒度聚合的更加细，但后面如果要更粗粒度的聚合时，就要对拼接的字符串进行拆分，处理会更麻烦，
         *
         */
//        KeyedStream<Tuple3<String, String, Double>, String> tuple3StringKeyedStream = keyedStream.keyBy(t -> t.f0 + t.f1);

        /**
         * 此处可以调用map方法，然后实现 MapRichFunction，
         * 而调用process，process方法是一个更加地城的方法
         */
        SingleOutputStreamOperator<Tuple3<String, String, Double>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {

            /**
             * transient: 是不参与反序列化，因为JobManager要将定义的mapState序列化后发送给TaskManager，那么后面反序列化后mapState就
             * 有值，而我们想要的是状态后端open之后，取出来的值，不让mapState参与序列化与反序列化。
             */
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
                Double totalMoney = historyMoney + money;       // 累加

                //更新到mapState中,数据仅更新内存
                mapState.put(city, totalMoney);

                //print
                value.f2 = totalMoney;

                out.collect((value));

            }
        });

        result.print();

        //启动执行
        env.execute("MapStateDemo01");

    }
}
