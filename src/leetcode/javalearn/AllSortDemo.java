package com.zzml.flinklearn.leetcode.others;

import java.util.Arrays;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:AllSortDemo
 * @Auther: zzml
 * @Description: 常见的排序算法汇总
 * @Date: 2023/2/13 23:12
 * @Version: v1.0
 * @ModifyDate:
 */

public class AllSortDemo {

    public static void main(String[] args) {

        int[] arr = {32, 53, 23, 13, 5, 29, 32, 12, 21, 19, 91};

        bubbleSort(arr);
        selectSorted(arr);

//        bubbleSortDesc(arr);

//        AllSortDemo allSortDemo = new AllSortDemo();
//        allSortDemo.bubbleSortDesc2(arr);

    }












    /**
     * 选择排序
     */
    public static void selectSorted(int[] arr){

        int arrLen = arr.length;

        for (int i = 0; i < arrLen; i++){
            int minIndex = i;   // 将第一个当作最小值的index
            for (int j = i; j < arrLen; j++){     // j = i，即从第i之后属于无需趣。i之前属于有序区；
                if (arr[j] < arr[minIndex]){
                    minIndex = j;
                }
            }
            // 调换位置
            int temp = arr[minIndex];
            arr[minIndex] = arr[i];
            arr[i] = temp;
        }

        System.out.println("选择排序结果为: "+ Arrays.toString(arr));

    }



    /**
     * 冒泡排序：
     * 1.比较相邻的元素，正序排序，把大的往后排；
     * 2.简单来说就是重复的对比两个元素，将大的放到后面
     */
    public static void bubbleSort(int[] arr) {
        int arrLen = arr.length;
        int tmp;

        /**
         * 第一个循环 i 是最外层的，即第一个数在冒泡的过程中一直走到最后，中途有比该数大的，换成这个数一直循环走到最后。
         * 所以内层的for循环才能参与比较。如果j是从0开始，则j<arrLen-i-1，-i：是指已经排序好的个数，-1是正常循环时标记是从0开始，需要-1。
         */
        for (int i = 0; i < arrLen - 1; i++) {
            for (int j = 0; j < arrLen - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    tmp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = tmp;
                }
            }
        }

        System.out.println("冒泡排序结果：" + Arrays.toString(arr));

    }

    /**
     * 冒泡排序；正序
     * 推荐系数：一般不用
     *
     * @param arr 输入数组
     */
    public static void bubbleSortDesc(int[] arr) {

        int length = arr.length;
        int temp;   // 用于存放交换时的数据
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }

        System.out.println("冒泡正序排序：" + Arrays.toString(arr));
    }

    /**
     * 快速排序
     * 推荐系数： 要求时间最快时使用。
     *
     * @param num   输入数组
     * @param start
     * @param end
     */
    public static void quickSort(int[] num, int start, int end) {


    }

    // 冒泡排序训练-001
    public void bubbleSortDesc2(int[] arr) {

        int length = arr.length;
        int temp;

        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    temp = arr[j + 1];
                    arr[j + 1] = arr[j];
                    arr[j] = temp;
                }
            }
        }

        System.out.println(Arrays.toString(arr));

    }

}
