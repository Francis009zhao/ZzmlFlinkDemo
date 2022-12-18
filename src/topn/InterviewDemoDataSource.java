package com.zzml.flinklearn.exer.topn;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:InterviewDemoDataSource
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/27 00:54
 * @Version: v1.0
 * @ModifyDate:
 */

public class InterviewDemoDataSource implements SourceFunction<InterviewDemoAppBean> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<InterviewDemoAppBean> ctx) throws Exception {

        Random random = new Random();

//        String[] appId = {};

        String[] appId = {"微信","拼多多","alibaba","huawei","淘宝"};
//                ,"jd","抖音","QQ",
//                "QQ音乐","bibi","闲鱼","爱奇艺","铁路12306","快手","美团","网易","央视频",
//                "高德地图","有道","微博","小红书","钉钉","百度","今日头条","腾讯视频"
//                ,"芒果视频","腾讯会议","百度云","华为云","东方财富"};

        while (flag){

            ctx.collect(new InterviewDemoAppBean(
                    appId[random.nextInt(appId.length)],
                    "AppName",
                    System.currentTimeMillis() + random.nextInt(100) * -1 + ""
            ));

            Thread.sleep(100);

        }


    }

    @Override
    public void cancel() {
        flag = false;
    }
}
