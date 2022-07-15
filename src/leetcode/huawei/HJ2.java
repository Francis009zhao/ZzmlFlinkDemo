package com.zzml.flinklearn.leetcode.huawei;

import java.util.Scanner;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ2
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/5 22:06
 * @Version: v1.0
 * @ModifyDate:
 */

public class HJ2 {
    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        String str = sc.nextLine().toLowerCase();
        String charA = sc.nextLine().toLowerCase();

        System.out.println(str.length() - str.replace(charA,"").length());


    }
}
