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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindowLeftOuterJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1000,A,1
        DataStreamSource<String> leftSteam = env.socketTextStream("localhost", 8888);
        //2000,A,2
        DataStreamSource<String> rightStream = env.socketTextStream("localhost", 9999);

        //提取第一个流中数据的EventTime
        DataStream<String> leftWaterMarkStream = leftSteam
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.split(",")[0]);
                    }
                });
        //提取第二个流中数据的EventTime
        DataStream<String> rightWaterMarkStream = rightStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.split(",")[0]);
                    }
                });
        //对第一个流整理成tuple3
        DataStream<Tuple3<Long, String, String>> leftTuple = leftWaterMarkStream.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(Long.parseLong(fields[0]), fields[1], fields[2]);
                    }
                }
        );
        //对第二个流整理成tuple3
        DataStream<Tuple3<Long, String, String>> rightTuple = rightWaterMarkStream.map(
                new MapFunction<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return Tuple3.of(Long.parseLong(fields[0]), fields[1], fields[2]);
                    }
                }
        );
        //调用coGroup实现left join
        DataStream<Tuple6<Long, String, String, Long, String, String>> joinedStream = leftTuple.coGroup(rightTuple)
                .where(new KeySelector<Tuple3<Long, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, String> value) throws Exception {
                        return value.f1;
                    }
                })
                .equalTo(new KeySelector<Tuple3<Long, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, String> value) throws Exception {
                        return value.f1;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new MyLeftOuterJoinFunction());

        joinedStream.print();

        env.execute();
    }
}
