package com.zzml.flinklearn.doitedu;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.expressions.In;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:StateDataStructureDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/23 20:21
 * @Version: v1.0
 * @ModifyDate:
 */

public class StateDataStructureDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        // 开启快照后，就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        DataStreamSource<String> source = env.socketTextStream("hadoop100", 9999);

        source.keyBy(s -> "0")
                .map(new RichMapFunction<String, String>() {

                    // 单值状态
                    ValueState<String> valueState;

                    // list状态
                    ListState<String> listState;

                    // mapState状态
                    MapState<String, String> mapState;

                    // reducing状态
                    ReducingState<Integer> reducingState;

                    // aggregate状态
                    AggregatingState<Integer, Double> aggState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();

                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("vState", String.class);
                        valueState = runtimeContext.getState(valueStateDescriptor);

                        // 获取一个List结构的状态存储器
                        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("lstState", String.class);
                        listState = runtimeContext.getListState(listStateDescriptor);

                        // 获取一个 Map 结构的状态存储器
                        mapState = runtimeContext.getMapState(new MapStateDescriptor<String, String>("mapState", String.class, String.class));

                        // 获取一个reduce聚合状态
                        reducingState = runtimeContext.getReducingState(new ReducingStateDescriptor<Integer>("reduceState", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Integer.class));

                        // 获取一个aggregate聚合状态
                        // 比如，我们要插入整数，返回平均值
                        aggState = runtimeContext.getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "aggState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f1 / (double) accumulator.f0;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return Tuple2.of(a.f0 + b.f0, a.f1 + a.f1);
                                    }
                                },
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                })
                        ));


                    }

                    @Override
                    public String map(String value) throws Exception {

                        // valueState的数据操作API
                        valueState.update("");  // 更新掉状态中的值
                        String str = valueState.value();  // 获取状态中的值

                        // listState的数据操作API
                        Iterable<String> strings = listState.get();   // 获取整个listState的数据迭代器
                        listState.add("a");  // 添加一个元素到liststate中

                        // 一次性放入多个元素到liststate中
                        listState.addAll(Arrays.asList("a", "b", "c"));
                        // 一次性将liststate中的数据替换为传入的元素
                        listState.update(Arrays.asList("1,", "2"));

                        /**
                         * mapState的数据操作api
                         */
                        String v = mapState.get("a");   // 从mapstate中根据一个key来获取它的value

                        boolean contain = mapState.contains("a");   // 判断mapstate中是否包含指定的key

                        Iterator<Map.Entry<String, String>> entryIterator = mapState.iterator();  // 拿到mapstate的entry迭代器

                        Iterable<Map.Entry<String, String>> entryIterable = mapState.entries(); // 拿到mapstate的entry的 Iterable（内含迭代器）

                        mapState.put("a", "100");   // 往mapstate中插入一对KV

                        boolean isEmpty = mapState.isEmpty();  // 判断mapstate中是否没有元素（是否为空）

                        HashMap<String, String> dataMap = new HashMap<>();
                        dataMap.put("a","1");
                        dataMap.put("b","2");

                        mapState.putAll(dataMap);  // 通过一个hashmap对象，来一次性放入多对KV到mapstate中

                        Iterable<String> keys = mapState.keys();  // 拿到mapstate中所有key
                        Iterable<String> values = mapState.values(); // 拿到mapstate中的所有value

                        mapState.remove("a"); // 从mapstate移除key=“a"的条目

                        /**
                         * reduce 状态使用
                         */
                        reducingState.add(10);  // 往聚合状态中添加数据  ,此刻状态中的数据是10
                        reducingState.add(20);  // 往聚合状态中添加数据  ,此刻状态中的数据是30
                        Integer stateValue = reducingState.get();  // 从聚合状态中获取数据值


                        /**
                         * aggreate 状态使用
                         */
                        aggState.add(10);
                        aggState.add(20);

                        Double avgDouble = aggState.get(); // 获取状态值 ： 15.0

                        return "";
                    }
                });


        env.execute();

    }
}
