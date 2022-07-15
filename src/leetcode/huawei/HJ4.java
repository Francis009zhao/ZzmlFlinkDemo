package com.zzml.flinklearn.leetcode.huawei;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:HJ4
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/11 22:21
 * @Version: v1.0
 * @ModifyDate:
 */

public class HJ4 {

    public static void main(String[] args) {

        /**
         * 1.需要输入字符串，用到Scanner和hasNext()。
         * （1）建立 Scanner sc = new Scanner(System.in);
         * （2）判断有无输入用sc.hasNext().接收字符串使用sc.nextLine().
         * 2.一次性接受全部的字符串，对8取余，获知需要补0的位数。使用StringBuilder中的append()函数进行字符串修改，别忘了toString()。
         *   字符串缓冲区的建立：StringBuilder sb = new StringBuilder();
         * 3.输出时，截取前8位进行输出，并更新字符串。用到str.substring()函数：
         * （1）str.substring(i)意为截取从字符索引第i位到末尾的字符串。
         * （2）str.substring(i,j)意为截取索引第i位到第（j-1）位字符串。包含i，不包含j。
         */
        Scanner sc = new Scanner(System.in);

        while (sc.hasNext()){

            String str = sc.nextLine();
            StringBuilder strBuilder = new StringBuilder();

            strBuilder.append(str);
            int size = str.length();
            int zeroNum = 8 - size % 8;
            while ((zeroNum > 0) && (zeroNum < 8)){
                strBuilder.append("0");
                zeroNum--;
            }
            String str1 = strBuilder.toString();
            while (str1.length() > 0){
                System.out.println(str1.substring(0,8));
                str1 = str1.substring(8);
            }

        }

    }

}
