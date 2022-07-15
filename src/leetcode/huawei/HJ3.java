package com.zzml.flinklearn.leetcode.huawei;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeSet;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ3
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/10 10:34
 * @Version: v1.0
 * @ModifyDate: 描述
 * 明明生成了NN个1到500之间的随机整数。请你删去其中重复的数字，即相同的数字只保留一个，把其余相同的数去掉，然后再把这些数从小到大排序，按照排好的顺序输出。
 * <p>
 * 数据范围： 1 \le n \le 1000 \1≤n≤1000  ，输入的数字大小满足 1 \le val \le 500 \1≤val≤500
 * 输入描述：
 * 第一行先输入随机整数的个数 N 。 接下来的 N 行每行输入一个整数，代表明明生成的随机数。 具体格式可以参考下面的"示例"。
 * 输出描述：
 * 输出多行，表示输入数据处理后的结果
 */

public class HJ3 {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        int num = sc.nextInt();
        TreeSet treeSet = new TreeSet();

        for (int i = 0; i < num; i++) {
            treeSet.add(sc.nextInt());
        }

        Iterator iterator = treeSet.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }


        HJ3 hj3 = new HJ3();
        hj3.numRand();


    }

    //先将读入的随机数排序，然后计数，如果比较发现当前数字与上一数字相同，则跳过该数字。
    public void numRand() {
        Scanner in = new Scanner(System.in);

        while (in.hasNext()) {
            int count = in.nextInt();
            int[] data = new int[count];
            for (int i = 0; i < data.length; i++) {
                data[i] = in.nextInt();
            }
            Arrays.sort(data);
            for (int i = 1; i < count; i++) {
                if (data[i] != data[i - 1]) {
                    System.out.println(data[i]);
                }
            }
        }
    }

}


