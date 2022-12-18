package com.zzml.flinklearn.exer.topn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Comparator;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:MyProcess
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/10 15:23
 * @Version: v1.0
 * @ModifyDate:
 */

/**
 * 这个方法是直接将所有数据放在一个分区上进行了开窗操作。这相当于将并行度强行设置为 1，在实际应用中是要尽量避免的，所以 Flink 官
 * 方也并不推荐使用 AllWindowedStream 进行处理
 */
public class MyProcess extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

    @Override
    public void process(Boolean aBoolean, Context context, Iterable<Event> iterable, Collector<String> out) throws Exception {

        HashMap<String, Long> urlCountMap = new HashMap<>();
        // 遍历窗口中数据，将浏览量保存到一个 HashMap 中
        for (Event event : iterable) {
            String url = event.url;
            if (urlCountMap.containsKey(url)) {
                long count = urlCountMap.get(url);
                urlCountMap.put(url, count + 1L);
            } else {
                urlCountMap.put(url, 1L);
            }
        }
        ArrayList<Tuple2<String, Long>> mapList = new ArrayList<Tuple2<String, Long>>();
        // 将浏览量数据放入ArrayList，进行排序
        for (String key : urlCountMap.keySet()) {
            mapList.add(Tuple2.of(key, urlCountMap.get(key)));
        }
        mapList.sort(new Comparator<Tuple2<String, Long>>() {
            @Override
            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                return o2.f1.intValue() - o1.f1.intValue();
            }
        });
        // 取排序后的前两名，构建输出结果
        StringBuilder result = new StringBuilder();
        System.out.println(urlCountMap);
        result.append("========================================\n");
        int size = urlCountMap.size();
        int top = Integer.min(size, 2);
//        for (int i = 0; i < top; i++) {
//            Tuple2<String, Long> temp = mapList.get(i);
//            String info = "浏览量No." + (i + 1) +
//                    " url：" + temp.f0 +
//                    " 浏览量：" + temp.f1 +
//                    " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";
//
//            result.append(info);
//        }

        /**
         * 必须修改问题：以下为改进版的数据输出，值相同的也输出，实现算法不是很好，找时间修改一下？？
         */
        for (int i = 0; i < top; i++) {
            Tuple2<String, Long> temp = mapList.get(i);
            String info;
            info = "浏览量No." + (i + 1) +
                    " url：" + temp.f0 +
                    " 浏览量：" + temp.f1 +
                    " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";
            result.append(info);
            if (i == top-1) {
                for (int j=top; j <size; j++){
                    Tuple2<String, Long> temp2 = mapList.get(j);
                    if (temp.f1 != temp2.f1){
                        break;
                    }
                    String info2 = "浏览量No." + (j) +
                            " url：" + temp2.f0 +
                            " 浏览量：" + temp2.f1 +
                            " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";
                    result.append(info2);
                }
            }

        }

        result.append("========================================\n");
        out.collect(result.toString());
    }
}
