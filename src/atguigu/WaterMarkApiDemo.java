package com.zzml.flinklearn.doitedu;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:WaterMarkApiDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/20 22:19
 * @Version: v1.0
 * @ModifyDate:
 */

public class WaterMarkApiDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<EventBean> eventDS = env.addSource(new MyCustomSourceFunction());

        eventDS.print(">>>");

        // 策略1： WatermarkStrategy.noWatermarks()  不生成 watermark，禁用了事件时间的推进机制
        // 策略2： WatermarkStrategy.forMonotonousTimestamps()  紧跟最大事件时间
        // 策略3： WatermarkStrategy.forBoundedOutOfOrderness()  允许乱序的 watermark生成策略
        // 策略4： WatermarkStrategy.forGenerator()  自定义watermark生成算法

        /*
         * 示例 一 ：  从最源头算子开始，生成watermark
         */
        // 1、构造一个watermark的生成策略对象（算法策略，及事件时间的抽取方法）
        // 1、构造一个watermark的生成策略对象（算法策略，及事件时间的抽取方法）
//        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
//                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))  // 允许乱序的算法策略
//                .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[2]));  // 时间戳抽取方法
        // 2、将构造好的 watermark策略对象，分配给流（source算子）
        /*s1.assignTimestampsAndWatermarks(watermarkStrategy);*/
        /**
         * 我们想生成一个延迟 3 s 的固定水印，可以这样做
         * dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)));
         */
        SingleOutputStreamOperator<EventBean> eventWithWaterMarkDS = eventDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))       // 允许乱序的算法策略
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getEventTs();
                    }
                }));
        /**
         * >>>> EventBean(tecName=hbase, num=3, eventTs=1658330648663)
         * 本次收到的数据EventBean(tecName=hbase, num=3, eventTs=1658330648663)
         * 此刻的watermark： -9223372036854775808
         * 此刻的处理时间（processing time）： 1658330649035
         * result>>>> EventBean(tecName=hbase, num=3, eventTs=1658330648663)
         * >>>> EventBean(tecName=hbase, num=1, eventTs=1658330649959)
         * 本次收到的数据EventBean(tecName=hbase, num=1, eventTs=1658330649959)
         * 此刻的watermark： 1658330648662
         * 此刻的处理时间（processing time）： 1658330650279
         * result>>>> EventBean(tecName=hbase, num=1, eventTs=1658330649959)
         * >>>> EventBean(tecName=hive, num=2, eventTs=1658330651280)
         * 本次收到的数据EventBean(tecName=hive, num=2, eventTs=1658330651280)
         * 此刻的watermark： 1658330649958
         * 此刻的处理时间（processing time）： 1658330651506
         * result>>>> EventBean(tecName=hive, num=2, eventTs=1658330651280)
         */

        /*
         * 示例 二：  不从最源头算子开始生成watermark，而是从中间环节的某个算子开始生成watermark
         * 注意！：如果在源头就已经生成了watermark， 就不要在下游再次产生watermark
         *
         * 单调递增生成水印 相当于上述的延迟策略去掉了延迟时间，以 event 中的时间戳充当了水印，可以这样使用：
         */
//        SingleOutputStreamOperator<EventBean> eventWithWaterMarkDS = eventDS.assignTimestampsAndWatermarks(WatermarkStrategy
//                .<EventBean>forMonotonousTimestamps()
//                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
//                    @Override
//                    public long extractTimestamp(EventBean element, long recordTimestamp) {
//                        return element.getEventTs();
//                    }
//                })).setParallelism(2);

        SingleOutputStreamOperator<EventBean> resultDS = eventWithWaterMarkDS.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean value, Context ctx, Collector<EventBean> out) throws Exception {

                long processingTime = ctx.timerService().currentProcessingTime();

                long currentWatermark = ctx.timerService().currentWatermark();

                System.out.println("本次收到的数据" + value);
                System.out.println("此刻的watermark： " + currentWatermark);
                System.out.println("此刻的处理时间（processing time）： " + processingTime );

                out.collect(value);
            }
        }).setParallelism(1);

        resultDS.print("result>>>");

        env.execute();

    }
}

