package com.zzml.flinklearn.leetcode.huawei;

import java.util.ArrayList;
import java.util.Scanner;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ6
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/13 22:12
 * @Version: v1.0
 * @ModifyDate:
 */

public class HJ6 {

    public static void main(String[] args) {

//        Scanner sc = new Scanner(System.in);
//        long num = sc.nextLong();
//        for (long i = 2; i <= num; ++i) {
//            while (num % i == 0) {
//                System.out.print(i + " ");
//                num /= i;
//            }
//        }
//        System.out.println();


        Scanner scanner = new Scanner(System.in);

        long num = scanner.nextLong();
        long k = (long) Math.sqrt(num);

        for (long i = 2; i <= k; ++i) {
            while (num % i == 0) {
                System.out.print(i + " ");
                num /= i;
            }
        }
        System.out.println(num == 1 ? "" : num + " ");
    }
}
