package com.zzml.flink.source;

import com.zzml.flink.bean.MyData;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class CreateMyData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MyData> sourceStream = env.addSource(new MyDataSource());
        env.setParallelism(3);
        sourceStream.print();
        env.execute();
    }


    private static class MyDataSource implements SourceFunction<MyData> {
        // 定义标志位，用来控制数据的产生
        private boolean isRunning = true;
        private final Random random = new Random(0);

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (isRunning) {
                ctx.collect(new MyData(random.nextInt(5), System.currentTimeMillis(), random.nextFloat()));
                Thread.sleep(1000L); // 1s生成1个数据
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
