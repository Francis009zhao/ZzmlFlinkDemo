package cn._51doit.flink.day07;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //奇数
        OutputTag<String> oddOutputTag = new OutputTag<String>("odd") {
        };
        //偶数
        OutputTag<String> evenOutputTag = new OutputTag<String>("even") {
        };
        //非数字
        OutputTag<String> nanOutputTag = new OutputTag<String>("nan") {
        };

        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 == 0) {
                        //偶数
                        ctx.output(evenOutputTag, value);
                    } else {
                        //奇数
                        ctx.output(oddOutputTag, value);
                    }
                } catch (NumberFormatException e) {
                    ctx.output(nanOutputTag, value);
                }
                //在主流中输出全部的数据
                out.collect(value);
            }
        });

        //偶数
        DataStream<String> evenStream = mainStream.getSideOutput(evenOutputTag);

        //奇数
        DataStream<String> oddStream = mainStream.getSideOutput(oddOutputTag);

        oddStream.print("odd: ");

        evenStream.print("even: ");

        mainStream.print("main: ");


        env.execute();
    }
}
