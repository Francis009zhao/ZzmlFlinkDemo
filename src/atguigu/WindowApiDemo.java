package com.zzml.flinklearn.doitedu;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:WindowApiDemo
 * @Auther: zzml
 * @Description: 窗口学习，汇总各种信息，注释等
 * @Date: 2022/7/20 23:38
 * @Version: v1.0
 * @ModifyDate:
 */

public class WindowApiDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> source = env.socketTextStream("hadoop100", 9999);
        DataStreamSource<EventBean> eventDS = env.addSource(new MyCustomSourceFunction());

        eventDS.print(">>>");

        // 分配 watermark ，以推进事件时间
        SingleOutputStreamOperator<EventBean> watermarkedBeanStream = eventDS.assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getEventTs();
                    }
                }));

        /**
         * 滚动聚合api使用示例
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
         * 使用aggregate算子来实现
         */
        SingleOutputStreamOperator<Integer> resultStream = watermarkedBeanStream.keyBy(EventBean::getTecName)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))  // 参数1： 窗口长度 ； 参数2：滑动步长
                // reduce ：滚动聚合算子，它有个限制 ，聚合结果的数据类型 与  数据源中的数据类型 ，是一致
//                .reduce(new ReduceFunction<EventBean>() {
//                    @Override
//                    public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
//                        value2.num = value1.num + value2.num;
//                        return value2;
//                    }
//                }).print();
                .aggregate(new AggregateFunction<EventBean, Integer, Integer>() {

                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 滚动聚合的逻辑（拿到一条数据，如何去更新累加器）
                     * @param value The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public Integer add(EventBean value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    /**
                     * 从累加器中，计算出最终要输出的窗口结算结果
                     * @param accumulator The accumulator of the aggregation
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    /**
                     *  批计算模式下，可能需要将多个上游的局部聚合累加器，放在下游进行全局聚合
                     *  因为需要对两个累加器进行合并
                     *  这里就是合并的逻辑
                     *  流计算模式下，不用实现！
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

//        resultStream.print("result>>>");

        /**
         * 需求 二 ：  每隔10s，统计最近 30s 的数据中，每个用户的平均每次行为时长
         * 要求用 aggregate 算子来做聚合
         * 滚动聚合api使用示例
         */
        // 查看原类

        /**
         * TODO 补充练习 1
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
         * 滑动聚合api使用示例
         * 使用sum算子来实现
         */
        watermarkedBeanStream.map(bean -> Tuple2.of(bean, 1)).returns(new TypeHint<Tuple2<EventBean, Integer>>() {})
                .keyBy(tp -> tp.f0.getTecName())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);
//                .print("result");

        /**
         * TODO 补充练习 2
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长
         * 滑动聚合api使用示例
         * 用max算子来实现
         */



        env.execute("WindowApiDemo");

    }
}
