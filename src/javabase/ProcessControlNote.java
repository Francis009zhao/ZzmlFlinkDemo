package com.zzml.javabases.process;

/**
 * 三种基本流程控制结构：
 *      顺序结构：
 *          程序从上到下逐行的执行，中间没有任何判断和跳转
 *
 *      分支结构：
 *          根据条件，选择性的执行某段代码
 *          有if...else和switch-case两种分支语句
 *
 *      循环结构：
 *          根据循环条件，重复性的执行某段代码
 *          有while、do...while、for三种循环语句
 *          foreach循环： 用于遍历集合、数组元素
 *
 */
public class ProcessControlNote {

    public static void main(String[] args) {

        /**
         * 一、if-else(三种结构)：
         *      1） if(condition){
         *
         *      }
         *
         *      2) if(condition){
         *          execute1
         *      }else{
         *          execute2
         *      }
         *
         *      3) if(condition1){
         *          execute1
         *      }else if(condition2){
         *          execute2
         *      }else if(condition3){
         *          execute3
         *      }
         *      ...
         *      else{
         *          executeN
         *      }
         */
        int heartBeats = 79;
        if (heartBeats < 60 || heartBeats > 100){
            System.out.println("需要进一步检查");
        }
        System.out.println("指标正常，检查结束！");

        int age = 23;
        if (age < 18){
            System.out.println("你还可以继续看动画片");
        }else {
            System.out.println("你可以看成人电影了");
        }




    }

}





































