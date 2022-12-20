package com.zzml.flinklearn.exer.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:CallableThreadTest
 * @Auther: zzml
 * @Description:
 * @Date: 2022/10/29 09:40
 * @Version: v1.0
 * @ModifyDate:
 */

public class CallableThreadDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        MyCallable m1 = new MyCallable();
        FutureTask<String> ft = new FutureTask<String>(m1);
        Thread thread = new Thread(ft);

        thread.start();
        System.out.println(ft.get());

    }
}

class MyCallable implements Callable<String>{

    @Override
    public String call() throws Exception {
        return "null";
    }
}
