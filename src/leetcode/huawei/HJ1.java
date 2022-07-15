package com.zzml.flinklearn.leetcode.huawei;

import java.util.Scanner;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ1
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/5 21:52
 * @Version: v1.0
 * @ModifyDate:
 * https://www.nowcoder.com/practice/8c949ea5f36f422594b306a2300315da?tpId=37&tqId=21224&rp=1&ru=/exam/oj/ta&qru=/exam/oj/ta&sourceUrl=%2Fexam%2Foj%2Fta%3Fpage%3D1%26tpId%3D37%26type%3D37&difficulty=undefined&judgeStatus=undefined&tags=&title=
 */

public class HJ1 {

    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);
        String str = input.nextLine();

        String[] s = str.split(" ");
        // 正则表达式实用性更好
//        str.split("\\s+");

        int length = s[s.length - 1].length();
        System.out.println(length);

    }

}
