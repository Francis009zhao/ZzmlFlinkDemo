package com.zzml.flinklearn.doit.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * KeyBy之后，用来存储K-V类型的状态，叫做KeyedState
 *
 * KeyedState种类有：ValueState<T> （value是一个基本类型、集合类型、自定义类型）
 * MapState<小K,V> （存储的是k，v类型）  （大K -> (小K， 小V)）
 * ListState（Value是一个list集合）
 *
 */

public class KeyedStateDemo01 {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置checkpoint
        env.enableCheckpointing(10000);
        //重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //source
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.68.100", 8888);

        //transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    if ("error".equals(word)) {
                        throw new RuntimeException("出现异常数据——error>>>>>>>>>");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);

        //调用map，取出每个分区中的数据
        keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> counter;

            @Override
            public void open(Configuration parameters) throws Exception {

                //想使用状态，先定义一个状态描述器（state的类型， 名称）
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-desc", Integer.class);

                //初始化或恢复历史状态,即处理状态描述器
                counter = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {

                String word = input.f0;
                Integer currentCount = input.f1;

                //从ValueState中取出历史次数
                Integer historyCount = counter.value();      //获取当前key对应的value

                if (historyCount == null){
                    historyCount = 0;
                }
                Integer total = historyCount + currentCount;    //累加

                //更新内存中的状态
                counter.update(total);

                input.f1 = total;       //累加后的次数

                return input;
            }
        }).print();

        //keyedStream.print();

        //执行
        env.execute("KeyedStateDemo01");


    }
}



























