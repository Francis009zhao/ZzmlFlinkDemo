package cn._51doit.flink.day05;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

public class MyRightOuterJoinFunction implements CoGroupFunction<
        Tuple3<Long, String, String>, //左流输入的数据类型
        Tuple3<Long, String, String>, //右流输入的数据类型
        Tuple6<Long, String, String, Long, String, String>> { //输出的数据类型
    @Override
    public void coGroup(Iterable<Tuple3<Long, String, String>> first,
                        Iterable<Tuple3<Long, String, String>> second,
                        Collector<Tuple6<Long, String, String, Long, String, String>> out) throws Exception {
        //循环右流的数据，如果有数据说明触发窗口时右流中有数据
        for (Tuple3<Long, String, String> right : second) {
            boolean hasJoined = false;
            //循环左流的数据，如果有数据说明触发窗口时左流中有数据，即join上流
            for (Tuple3<Long, String, String> left : first) {
                //返回两个流join上的数据
                out.collect(Tuple6.of(left.f0, left.f1, left.f2, right.f0, right.f1, right.f2));
                hasJoined = true;
            }
            //如果没有join上，只返回右流的数据
            if (!hasJoined) {
                out.collect(Tuple6.of(null, null, null, right.f0, right.f1, right.f2));
            }
        }
    }
}