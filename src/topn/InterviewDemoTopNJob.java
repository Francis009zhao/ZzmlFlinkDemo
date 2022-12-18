package com.zzml.flinklearn.exer.topn;

import com.zzml.flink.utils.TimeFormatUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.TreeMap;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:InterviewDemoTopNJob
 * @Auther: zzml
 * @Description: 面试对应的demo，实际生产环境可直接使用
 * @Date: 2022/11/26 22:42
 * @Version: v1.0
 * @ModifyDate:
 */

public class InterviewDemoTopNJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<InterviewDemoAppBean> appBeanDS = env.addSource(new InterviewDemoDataSource());

//        appBeanDS.print("Data===>>>>>");
//        InterviewDemoAppBean interviewDemoAppBean = new InterviewDemoAppBean();

        SingleOutputStreamOperator<InterviewDemoAppCountBean> result = appBeanDS.keyBy(InterviewDemoAppBean::getAppId)
                .process(new InterviewDemoAppKeyedProcessFunction())
                .setParallelism(1);


        result.print(">>>");


        env.execute();


    }
}
