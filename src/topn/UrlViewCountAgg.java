package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:UrlViewCountAgg
 * @Auther: zzml
 * @Description: 自定义增量聚合
 * @Date: 2022/11/10 10:47
 * @Version: v1.0
 * @ModifyDate:
 */

public class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
