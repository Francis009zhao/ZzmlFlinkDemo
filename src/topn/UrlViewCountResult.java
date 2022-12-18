package com.zzml.flinklearn.exer.topn;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:UrlViewCountResult
 * @Auther: zzml
 * @Description: 自定义全窗口函数，只需要包装窗口信息
 * @Date: 2022/11/10 10:48
 * @Version: v1.0
 * @ModifyDate:
 */

public class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {

        // 结合窗口信息，包装输出内容
        long startTs = context.window().getStart();
        long endTs = context.window().getEnd();

        out.collect(new UrlViewCount(url, elements.iterator().next(), startTs, endTs));

    }
}
