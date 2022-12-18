package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:TopN
 * @Auther: zzml
 * @Description: 自定义处理函数，排序取top n
 * @Date: 2022/11/10 15:02
 * @Version: v1.0
 * @ModifyDate:
 */

public class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {

    private Integer n;
    // 定义一个列表状态 //为什么这里不直接赋值，因为还没有获取到上下文环境
    private ListState<UrlViewCount> urlViewCountListState;

    public TopN(Integer n) {
        this.n = n;
    }


    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<UrlViewCount> urlViewCountListStateDescriptor = new ListStateDescriptor<>("url-view-count-list", Types.POJO(UrlViewCount.class));

        // 从环境中获取列表的状态句柄
        urlViewCountListState = getRuntimeContext().getListState(urlViewCountListStateDescriptor);
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {

        // 将count数据添加到列表状态中，
        urlViewCountListState.add(value);
        // 注册 window end + 1ms后的定时器，等待所有数据到齐开始排序
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        // 将数据从列表状态变量中取出，放入ArrayList，方便排序
        ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
        for (UrlViewCount urlViewCount : urlViewCountListState.get()){
            urlViewCountArrayList.add(urlViewCount);
        }

        // 已经将数据写入array数组，可以清理状态
        urlViewCountListState.clear();

        // 排序
        urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
                return o2.count.intValue() - o1.count.intValue();
            }
        });

        // 取出topN
        StringBuilder result = new StringBuilder();

        result.append("========================================\n");
        result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");

        int size = urlViewCountArrayList.size();
        int topN = Integer.min(size, this.n);      // 健壮性，如果窗口触发时，arrayList里面的元素个数不够n，就会报数组越界错误

        for (int i = 0; i < topN; i++){
            UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
            String info = "No." + (i + 1) + " "
                    + "url：" + urlViewCount.url + " "
                    + "浏览量：" + urlViewCount.count + "\n";

            result.append(info);
        }

        result.append("========================================\n");
        out.collect(result.toString());

    }

}
