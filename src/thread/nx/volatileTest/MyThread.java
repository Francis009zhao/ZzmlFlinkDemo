package com.zzml.flinklearn.exer.thread.nx.volatileTest;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:MyThread
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/28 22:14
 * @Version: v1.0
 * @ModifyDate:
 */

public class MyThread extends Thread{

    // volatile 不具备原子性
    public static int count;

    private static void addCount(){
        for (int i=0; i< 100; i++){
            count++;
        }
        System.out.println("count=" + count);
    }

    @Override
    public void run() {
        addCount();
    }
}
