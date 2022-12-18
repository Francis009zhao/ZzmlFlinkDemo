package com.zzml.flinklearn.exer.topn;

import com.zzml.flink.utils.TimeFormatUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:InterviewDemoAppKeyedProcessFunction
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/27 01:22
 * @Version: v1.0
 * @ModifyDate:
 */

public class InterviewDemoAppKeyedProcessFunction extends KeyedProcessFunction<String, InterviewDemoAppBean, InterviewDemoAppCountBean> {

    private MapState<String, Long> mapState;

    InterviewDemoAppCountBean result = null;

    // 定义processTime定时器的状态存储器
    private ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<String, Long> timeMapState = new MapStateDescriptor<>("timeMapState", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(timeMapState);

        // 定义valueState状态描述器
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class, 0L));

    }

    @Override
    public void processElement(InterviewDemoAppBean value, Context ctx, Collector<InterviewDemoAppCountBean> out) throws Exception {

        String ts = TimeFormatUtil.yyyyMMddHHmm(value.getEventTs());

        if (mapState.contains(ts)) {
            Long cnt = mapState.get(ts);
            mapState.put(ts, cnt + 1);
        } else {
            mapState.put(ts, 1L);
        }

        result = InterviewDemoTopN.topN(3, mapState.get(ts), value);
//        out.collect(result);

        // 获取定时器的时间
//        Long timerTs = timerTsState.value();
//
//        if (!mapState.contains(ts) && timerTs == 0L) {
//            long triggerTs = ctx.timerService().currentProcessingTime() + 5000;
//            ctx.timerService().registerProcessingTimeTimer(triggerTs);
//            // 更新定时器
//            timerTsState.update(triggerTs);
//            result = InterviewDemoTopN.topN(3, mapState.get(ts), value);
//        }

//        out.collect(new InterviewDemoAppCountBean(
//                value.getAppId(),
//                mapState.get(ts),
//                ts
//        ));

        //interviewDemoAppCountBean = InterviewDemoTopN.topN(3, mapState.get(ts), value);
//        interviewDemoAppBeanListState.add(value);

        // out.collect(InterviewDemoTopN.topN(3, mapState.get(ts), value));

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InterviewDemoAppCountBean> out) throws Exception {

        out.collect(result);

//        out.collect(InterviewDemoTopN.topN(3, mapState.get(ts), interviewDemoAppBeans));
        timerTsState.clear();

        System.out.println("--------------------------------------------");

//        mapState.clear();

    }

    @Override
    public void close() throws Exception {
        mapState.clear();
    }
}
