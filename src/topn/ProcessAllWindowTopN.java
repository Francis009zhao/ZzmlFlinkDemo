package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:ProcessAllWindowTopN
 * @Auther: zzml
 * @Description:
 * @Date: 2022/12/9 14:51
 * @Version: v1.0
 * @ModifyDate:
 */

public class ProcessAllWindowTopN {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> sourceDS = env.addSource(new CustomClickSource());

//        sourceDS.print("source>>>");

        // 提取事件时间——waterMark
        SingleOutputStreamOperator<Event> eventDS = sourceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));


        // 只需要统计url的数据量，即只需取出url值即可
        AllWindowedStream<String, TimeWindow> windowedStream = eventDS.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.url;
            }
        }).windowAll(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(60)));   // 开滑动窗口，去统计最近10s内的浏览量

//        windowedStream.process(new ProcessAllWindowFunction<String, Integer, TimeWindow>() {
//            @Override
//            public void process(Context context, Iterable<String> elements, Collector<Integer> out) throws Exception {
//                ArrayList<String> listCnt = new ArrayList<>();
//                while (elements.iterator().hasNext()){
//                    listCnt.add(elements.iterator().next());
//                }
//
//                out.collect(listCnt.size());
//            }
//        }).print("===========");


        SingleOutputStreamOperator<String> result = windowedStream
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        HashMap<String, Long> urlCountMap = new HashMap<>();

                        int itCnt = 0;

                        // 遍历窗口中的数据，将浏览量保存到一个HashMap中
                        for (String url : elements) {
                            itCnt++;
                            if (urlCountMap.containsKey(url)) {
                                Long cnt = urlCountMap.get(url);
                                urlCountMap.put(url, cnt + 1L);
                            } else {
                                urlCountMap.put(url, 1L);
                            }
                        }

                        // 将hashMap传入array进行排序
                        ArrayList<Tuple2<String, Long>> mapList = new ArrayList<>();
                        for (String key : urlCountMap.keySet()) {
                            mapList.add(Tuple2.of(key, urlCountMap.get(key)));
                        }
                        mapList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });

                        // 将排序后的结果输出
                        StringBuilder str = new StringBuilder();
                        str.append("========================================\n");
                        for (int i = 0; i < 3; i++) {
                            Tuple2<String, Long> temp = mapList.get(i);
                            String info = "浏览量 No." + (i + 1) +
                                    " url：" + temp.f0 +
                                    " 浏览量：" + temp.f1 +
                                    " 窗 口 结 束 时 间："
                                    +
                                    new Timestamp(context.window().getEnd()) + "\n";
                            str.append(info);
                        }
                        str.append(itCnt);
                        str.append("========================================\n");
                        out.collect(str.toString());

                    }
                });

        result.print(">>>");


        env.execute("ProcessAllWindowTopN");


    }

}
