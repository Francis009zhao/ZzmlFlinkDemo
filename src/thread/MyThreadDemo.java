package com.zzml.flinklearn.exer.thread;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:MyThreadDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/16 14:41
 * @Version: v1.0
 * @ModifyDate:
 */

public class MyThreadDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("", 9999);


        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 4, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("Thread-001");
                        return thread;
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());


        executor.execute(() -> {
            for (int i=0; i< 10; i++){
                System.out.println(i);
            }
        });

    }
}
