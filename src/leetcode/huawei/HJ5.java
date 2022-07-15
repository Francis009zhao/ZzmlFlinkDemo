package com.zzml.flinklearn.leetcode.huawei;

import java.util.Scanner;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ5
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/12 23:06
 * @Version: v1.0
 * @ModifyDate:
 */

public class HJ5 {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        while(sc.hasNextLine()){
            String s = sc.nextLine();
            System.out.println(Integer.parseInt(s.substring(2,s.length()),16));
        }

    }

}
