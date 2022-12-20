package com.zzml.flinklearn.exer.thread;

import java.util.concurrent.*;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:CallableRunnableTest
 * @Auther: zzml
 * @Description: Callable和Runnable创建多线程方式对比
 * @Date: 2022/10/17 15:54
 * @Version: v1.0
 * @ModifyDate:
 */

public class CallableRunnableTest {

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        Callable<String> callable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "callable" + Thread.currentThread().getName();
            }
        };

        // 支持泛型
        Future<String> futureCallable = executorService.submit(callable);

        try {
            System.out.println("获取callable的返回结果:" + futureCallable.get());
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ExecutionException e){
            e.printStackTrace();
        }

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println("runnable");
            }
        };

        executorService.execute(runnable);

        Future<?> futureRunnable = executorService.submit(runnable);

        try {
            System.out.println("获取runnable返回的结果: " + futureRunnable.get());
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ExecutionException e){
            e.printStackTrace();
        }

        executorService.shutdown();

    }
}
