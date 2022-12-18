package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:KeyedProcessTopN
 * @Auther: zzml
 * @Description:
 * @Date: 2022/12/9 18:08
 * @Version: v1.0
 * @ModifyDate:
 */

public class KeyedProcessTopN {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        SingleOutputStreamOperator<Event> eventStream = env.addSource(new CustomClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 按url来分组，求出每个url的访问量
//        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventStream.keyBy(k -> k.url)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        // 滚动窗口
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventStream.keyBy(k -> k.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(dt -> dt.windowEnd)
                .process(new TopN(5));

        result.print(">>>");

        eventStream.keyBy(dt -> dt.url)
                .process(new MapStateKeyUrlCntDemo(10000L))
                .print("URL>>>");

        env.execute("KeyedProcessTopN");

    }
}
