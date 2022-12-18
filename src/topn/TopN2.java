package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:TopN2
 * @Auther: zzml
 * @Description:
 * @Date: 2022/12/9 22:09
 * @Version: v1.0
 * @ModifyDate:
 */

public class TopN2 extends KeyedProcessFunction<Long, UrlViewCount, String> {

    private ListState<UrlViewCount> urlViewCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 定义状态描述器
        ListStateDescriptor<UrlViewCount> listStateDescriptor = new ListStateDescriptor<>("urlListState", Types.POJO(UrlViewCount.class));

        urlViewCountListState = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {

        urlViewCountListState.add(value);

        ctx.timerService().registerProcessingTimeTimer(ctx.getCurrentKey() + 1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();

        for (UrlViewCount urlViewCount : urlViewCountListState.get()){
            urlViewCountArrayList.add(urlViewCount);
        }

        urlViewCountListState.clear();

        urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
                return o2.count.intValue() - o1.count.intValue();
            }
        });


    }
}
