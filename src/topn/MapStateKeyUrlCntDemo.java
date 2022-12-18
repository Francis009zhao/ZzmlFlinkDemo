package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:MapStateKeyUrlCntDemo
 * @Auther: zzml
 * @Description: 基于mapstate来统计url的pv值
 * @Date: 2022/12/10 22:41
 * @Version: v1.0
 * @ModifyDate:
 */

public class MapStateKeyUrlCntDemo extends KeyedProcessFunction<String, Event, String> {

    private Long windowSize;

    //定义状态，用来保存每个窗口中统计的count值, 每个窗口计算的pv实际上是每个url的，即url拥有多个不同的窗口
    private MapState<Long, Long> windowUrlCountMapState;

    public MapStateKeyUrlCntDemo(Long windowSize){
        this.windowSize = windowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("url-window-count", Long.class, Long.class));

    }

    @Override
    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
        //每来一条数据，根据时间戳判断属于那个窗口（窗口分配器）
        Long windowStart = value.timestamp / windowSize * windowSize;
        Long windowEnd = windowStart + windowSize;

        //注册 end - 1 的定时器
        ctx.timerService().registerProcessingTimeTimer(windowEnd - 1);

        // 更新状态，进行增量聚合
        if (windowUrlCountMapState.contains(windowStart)){
            Long count = windowUrlCountMapState.get(windowStart);
            windowUrlCountMapState.put(windowStart, count + 1);
        }else {
            windowUrlCountMapState.put(windowStart, 1L);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        //定时器触发时输出计算结果
        Long windowEnd = timestamp + 1;
        Long windowStart = windowEnd - windowSize;

        Long count = windowUrlCountMapState.get(windowStart);

        out.collect("窗口:"+new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                +" url: " + ctx.getCurrentKey()
                + " count: " + count
        );

        //模拟窗口的关闭，清空map状态中的值
        windowUrlCountMapState.remove(windowStart);

    }
}
