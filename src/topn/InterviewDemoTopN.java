package com.zzml.flinklearn.exer.topn;

import com.zzml.flink.utils.TimeFormatUtil;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:InterviewDemoTopN
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/27 15:52
 * @Version: v1.0
 * @ModifyDate:
 */

public class InterviewDemoTopN {

//    private Long cnt;
//
//    InterviewDemoAppBean interviewDemoAppBean;
//
//    public InterviewDemoTopN() {
//    }
//
//    public InterviewDemoTopN(Long cnt, InterviewDemoAppBean interviewDemoAppBean) {
//        this.cnt = cnt;
//        this.interviewDemoAppBean = interviewDemoAppBean;
//    }

    public static InterviewDemoAppCountBean topN(int n, Long cnt, InterviewDemoAppBean interviewDemoAppBean) {

        InterviewDemoAppCountBean result = null;

        TreeMap<Long, InterviewDemoAppBean> treeMap = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long x, Long y) {
                return (x < y) ? -1 : 1;
            }
        });

        treeMap.put(cnt, interviewDemoAppBean);

        if (treeMap.size() > n) {
            treeMap.pollLastEntry();
        }

        for (Map.Entry<Long, InterviewDemoAppBean> entry : treeMap.entrySet()) {
            result = new InterviewDemoAppCountBean(
                    entry.getValue().appId,
                    entry.getKey(),
                    TimeFormatUtil.yyyyMMddHHmm(entry.getValue().eventTs)
            );
        }

        return result;
    }

}
