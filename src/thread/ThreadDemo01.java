package com.zzml.flinklearn.exer.thread;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:ThreadDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/10/29 09:31
 * @Version: v1.0
 * @ModifyDate:
 */

public class ThreadDemo01 {

    public static void main(String[] args) {

        ThreadTest t1 = new ThreadTest();

        t1.start();

    }

}

class ThreadTest extends Thread{

    @Override
    public void run() {
        System.out.println("继承Thread");
    }
}
