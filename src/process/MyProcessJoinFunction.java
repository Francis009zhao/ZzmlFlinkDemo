package com.zzml.flinklearn.doit.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;


public class MyProcessJoinFunction extends ProcessJoinFunction<
        Tuple3<Long, String, String>, //第一个数据流（左流）输入的数据类型
        Tuple3<Long, String, String>, //第二个数据流（右流）输入的数据类型
        Tuple6<Long, String, String, Long, String, String>> { //join后输出的数据类型
    @Override
    public void processElement(Tuple3<Long, String, String> left, //左流输入的一条数据
                               Tuple3<Long, String, String> right, //右流输入的一条数据
                               Context ctx, //上下文信息，可以获取各个流的timestamp和侧流输出的output
                               //用来输出join上的数据的Collector
                               Collector<Tuple6<Long, String, String, Long, String, String>> out)
            throws Exception {
        //将join上的数据添加到Collector中输出
        out.collect(Tuple6.of(left.f0, left.f1, left.f2, right.f0, right.f1, right.f2));
    }
}
