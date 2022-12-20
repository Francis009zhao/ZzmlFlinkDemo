package com.zzml.flinklearn.exer.thread.nx.volatileTest;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:Run
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/28 22:16
 * @Version: v1.0
 * @ModifyDate:
 */

public class Run {

    public static void main(String[] args) {

        MyThread[] myThread = new MyThread[100];

        for (int i=0; i<100; i++){
            myThread[i] = new MyThread();
        }

        for (int i=0; i<100; i++){
            myThread[i].start();
        }



    }
}

