package cn._51doit.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class IntervalJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1000,A,1
        DataStreamSource<String> leftLines = env.socketTextStream("localhost", 8888);
        //2000,A,2
        DataStreamSource<String> rightLines = env.socketTextStream("localhost", 9999);

        //提取第一个流中数据的EventTime
        DataStream<String> leftWaterMarkStream = leftLines
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.split(",")[0]);
                    }
                });
        //提取第二个流中数据的EventTime
        DataStream<String> rightWaterMarkStream = rightLines
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.split(",")[0]);
                    }
                });
        //对第一个流整理成tuple3
        DataStream<Tuple3<Long, String, String>> leftStream = leftWaterMarkStream.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(Long.parseLong(fields[0]), fields[1], fields[2]);
                    }
                }
        );
        //对第二个流整理成tuple3
        DataStream<Tuple3<Long, String, String>> rightStream = rightWaterMarkStream.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(Long.parseLong(fields[0]), fields[1], fields[2]);
                    }
                }
        );
        DataStream<Tuple6<Long, String, String, Long, String, String>> joinedStream = leftStream
                .keyBy(t -> t.f1) //指定第一个流分组KeySelector
                .intervalJoin(rightStream.keyBy(t -> t.f1)) //调用intervalJoin方法并指定第二个流的分组KeySelector
                .between(Time.seconds(-1), Time.seconds(1)) //设置join的时间区间范围为当前数据时间±1秒
                .upperBoundExclusive() //默认join时间范围为前后都包括的闭区间，现在设置为前闭后开区间
                .process(new MyProcessJoinFunction()); //调用process方法中传入自定义的MyProcessJoinFunction
        joinedStream.print(); //调用print sink 输出结果
        env.execute("IntervalJoinDemo");
    }
}
