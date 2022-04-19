package cn._51doit.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SlidingWindowJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置EventTime为时间标准
        Time maxOutOfOrderness = Time.milliseconds(0); //乱序延迟时间为0毫秒
        DataStreamSource<String> leftLines = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> rightLines = env.socketTextStream("localhost", 9999);
        //提取第一个流中数据的timestamp并生成watermark
        DataStream<String> leftWaterMarkStream = leftLines.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(maxOutOfOrderness) { //指定延迟时间
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.split(",")[0]);
                    }
                });
        //提取第二个流中数据的timestamp并生成watermark
        DataStream<String> rightWaterMarkStream = rightLines.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(maxOutOfOrderness) { //指定延迟时间
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.split(",")[0]);
                    }
                });
        //将第一个流整理成Tuple3，数据分别为左流的时间，左流join的key，左流的其他数据
        DataStream<Tuple3<Long, String, String>> leftStream = leftWaterMarkStream.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(Long.parseLong(fields[0]), fields[1], fields[2]);
                    }
                }
        );
        //将第二个流整理成Tuple3，数据分别为右流的时间，右流join的key，右流的其他数据
        DataStream<Tuple3<Long, String, String>> rightStream = rightWaterMarkStream.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(Long.parseLong(fields[0]), fields[1], fields[2]);
                    }
                }
        );

        //第一个流（左流）调用join方法关联第二个流（右流），并且在where方法和equalTo方法中分别指定两个流join的条件
        DataStream<Tuple6<Long, String, String, Long, String, String>> joinedStream = leftStream.join(rightStream)
                .where(new KeySelector<Tuple3<Long, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, String> value) throws Exception {
                        return value.f1; //将左流tuple3中的f1作为join的key
                    }
                })
                .equalTo(new KeySelector<Tuple3<Long, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, String> value) throws Exception {
                        return value.f1; //将右流tuple3中的f1作为join的key
                    }
                })
                //划分EventTime滑动窗口，窗口长度为10秒，5秒钟滑动一次
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new MyInnerJoinFunction()); //在apply方法中传入自定义的MyInnerJoinFunction
        joinedStream.print(); //调用print sink 输出结果
        env.execute("SlidingWindowJoinDemo");
    }
}
