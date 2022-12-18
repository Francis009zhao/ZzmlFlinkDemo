package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.scala.async.AsyncFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:AdsUrlTopNJob
 * @Auther: zzml
 * @Description: 需求：求每隔2s之前10s内的url的访问量的topN
 * @Date: 2022/11/9 16:25
 * @Version: v1.0
 * @ModifyDate:
 */

public class AdsUrlTopNJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // source——自定义
        DataStreamSource<Event> eventDS = env.addSource(new CustomClickSource());

        eventDS.print("eventDS>>>>>>>>");

        // 提取水位线
        SingleOutputStreamOperator<Event> eventStream = eventDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 需要按照url分组，求出每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventStream.keyBy(dt -> dt.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        // 按窗口结束时间分组，将不同分组数据放到一起来进行排序求topN
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd).process(new TopN(5));



        result.print("result");

        // process 处理
//        SingleOutputStreamOperator<String> process = eventStream
//                .keyBy(data -> true) //这里是划分到一个并行度了
//                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(20)))
//                .process(new MyProcess());
//        process.print("process");





        env.execute("AdsUrlTopNJob");

    }

}
