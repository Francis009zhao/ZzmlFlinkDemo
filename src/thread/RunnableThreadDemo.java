package com.zzml.flinklearn.exer.thread;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:RunnableThreadDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/10/29 09:35
 * @Version: v1.0
 * @ModifyDate:
 */

public class RunnableThreadDemo {

    public static void main(String[] args) {

        MyRunnable r1 = new MyRunnable();
        Thread t1 = new Thread(r1);

        Thread t2 = new Thread(r1);

//        t1.setName("thread001");
        t1.start();
        t2.start();

    }

}


class MyRunnable implements Runnable{

    @Override
    public void run() {
        System.out.println("实现Runnable");
    }
}