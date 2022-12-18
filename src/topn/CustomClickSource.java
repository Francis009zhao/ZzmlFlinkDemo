package com.zzml.flinklearn.exer.topn;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:CustomClickSource
 * @Auther: zzml
 * @Description: 自定义点击数据源
 * @Date: 2022/11/9 17:05
 * @Version: v1.0
 * @ModifyDate:
 */

public class CustomClickSource implements SourceFunction<Event> {

    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();

        String[] users = {"Mary", "Alice", "Bob", "Cary", "Frank"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2", "./prod?id=3", "./prod?id=4", "./prod?id=5"
                , "./prod?id=6", "./prod?id=7", "./prod?id=8", "./prod?id=9", "./prod?id=10", "./prod?id=11", "./prod?id=12"
                , "./prod?id=13", "./prod?id=14", "./prod?id=15", "./prod?id=16", "./prod?id=17"};

        while (running) {

            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));

            Thread.sleep(10);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
