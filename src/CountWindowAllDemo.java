package cn._51doit.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

//不分组，划分窗口
//按照数据的条数划分窗口，CountWindow属于GlobalWindow的一种
public class CountWindowAllDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1
        //2
        //3
        //socketTextStream返回的DataStream并行度为1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将字符串转成数字
        //本地执行，并行度为4，所以调用完map后返回的DataStream并行度为4
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //划分window
        //GlobalWindow有几个并行？并行度为：1，只有一个分区（在这个窗口内只有一个subTask）
        AllWindowedStream<Integer, GlobalWindow> windowed = nums.countWindowAll(5);

        //把窗口内的数据进行聚合
        SingleOutputStreamOperator<Integer> sum = windowed.sum(0);

        sum.print();

        env.execute();

    }
}
