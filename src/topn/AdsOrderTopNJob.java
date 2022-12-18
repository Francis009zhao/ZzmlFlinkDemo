package com.zzml.flinklearn.exer.topn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zzml.flink.connectors.KafkaConnectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:AdsOrderTopNJob
 * @Auther: zzml
 * @Description: 订单top10
 * @Date: 2022/11/9 12:01
 * @Version: v1.0
 * @ModifyDate:
 */

public class AdsOrderTopNJob {

    public static void main(String[] args) throws Exception {

        /**
         * 核心逻辑步骤：
         *      1.提前waterMark；
         *      2.按用户id来keyBy
         *      3.基于processTime，定义一个滑动窗口，窗口大小5分钟，步长20s，对数据进行一次聚合；
         *      4.使用windowAll（并发为1的特殊操作）将所有元素汇集到一个窗口内进行topN的计算(基于滚动窗口+事件时间)；
         *      5.java util工具包中的TreeMap来对key进行排序，设置TreeMap的大小为10，即可对value进行排序，对新来的数据对TreeMap的结果进行排序，删除小的数据
         *      6.最后将数据写入redis，redis中存储的数据结构为hash。
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30000L);

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaConnectors.getKafkaConsumer("order_detail", "order_detail_20221109");

        DataStreamSource<String> orderMsgDS = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<OrderDetailBean> orderDetailDS = orderMsgDS.map(new MapFunction<String, OrderDetailBean>() {
            @Override
            public OrderDetailBean map(String value) throws Exception {
                return JSON.parseObject(value,OrderDetailBean.class);
            }
        });

        SingleOutputStreamOperator<OrderDetailBean> orderDetailWaterMarkDS = orderDetailDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetailBean>() {

            private Long currentTimeStamp = 0L;

            private Long maxOutOfOrderNess = 5000L;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp - maxOutOfOrderNess);
            }

            @Override
            public long extractTimestamp(OrderDetailBean element, long recordTimestamp) {
                return element.getTimeStamp();
            }
        });

        SingleOutputStreamOperator<OrderDetailBean> reduceOrderDetailDS = orderDetailWaterMarkDS
                .keyBy(value -> value.getUserId())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetailBean>() {
                    @Override
                    public OrderDetailBean reduce(OrderDetailBean value1, OrderDetailBean value2) throws Exception {
                        return new OrderDetailBean(
                                value1.getUserId(),
                                value1.getItemId(),
                                value1.getCityName(),
                                value1.getPrice() + value2.getPrice(),
                                value1.getTimeStamp()
                        );
                    }
                });

        SingleOutputStreamOperator<Tuple2<Double, OrderDetailBean>> processOrderDetailDS = reduceOrderDetailDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetailBean, Tuple2<Double, OrderDetailBean>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<OrderDetailBean> elements, Collector<Tuple2<Double, OrderDetailBean>> out) throws Exception {
                        TreeMap<Double, OrderDetailBean> treeMap = new TreeMap<>(new Comparator<Double>() {
                            @Override
                            public int compare(Double x, Double y) {
                                return (x < y) ? -1 : 1;
                            }
                        });

                        Iterator<OrderDetailBean> iterator = elements.iterator();
                        if (iterator.hasNext()) {
                            treeMap.put(iterator.next().getPrice(), iterator.next());
                            if (treeMap.size() > 10) {
                                treeMap.pollLastEntry();
                            }
                        }

                        for (Map.Entry<Double, OrderDetailBean> entry : treeMap.entrySet()) {
                            out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                        }

                    }
                });

        processOrderDetailDS.print();

        env.execute("AdsOrderTopNJob");

    }

}
