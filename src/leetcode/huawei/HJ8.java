package com.zzml.flinklearn.leetcode.huawei;

import java.util.HashMap;
import java.util.Scanner;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ8
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/14 22:01
 * @Version: v1.0
 * @ModifyDate:
 */

public class HJ8 {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        int tableSize = sc.nextInt();
        HashMap<Integer, Integer> table = new HashMap<>(tableSize);

        for (int i = 0; i < tableSize; i++) {
            int key = sc.nextInt();
            int value = sc.nextInt();
            if (table.containsKey(key)){
                table.put(key, table.get(key) + value);
            }else {
                table.put(key, value);
            }
        }
        for (Integer key : table.keySet()){
            System.out.println(key + " " + table.get(key));
        }

    }
}
