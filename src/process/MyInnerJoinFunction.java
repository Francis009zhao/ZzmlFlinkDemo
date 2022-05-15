package com.zzml.flinklearn.doit.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;

public class MyInnerJoinFunction implements JoinFunction<
        Tuple3<Long, String, String>, //第一个数据流（左流）输入的数据类型
        Tuple3<Long, String, String>, //第二个数据流（右流）输入的数据类型
        Tuple6<Long, String, String, Long, String, String>> { //join后输出的数据类型
    //第一个流和第二个流输入的数据在同一个时间窗口内并且join的key相同才会调用join方法
    @Override
    public Tuple6<Long, String, String, Long, String, String> join(
            Tuple3<Long, String, String> left, //第一个数据流（左流）输入的一条数据
            Tuple3<Long, String, String> right) //第二个数据流（右流）输入的一条数据
            throws Exception {
        //能join将两个流的数据放入tuple6中，并返回输出
        return Tuple6.of(left.f0, left.f1, left.f2, right.f0, right.f1, right.f2);
    }
}
