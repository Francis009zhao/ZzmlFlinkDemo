package com.zzml.flinklearn.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zzml.flinklearn.atguigu.utils.DateFormatUtil;
import com.zzml.flinklearn.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:BaseLogApp
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/28 23:18
 * @Version: v1.0
 * @ModifyDate:
 */

//数据流：web/app -> nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：  Mock -> f1.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_log", "base_log"));

        //TODO 3.将数据转换为JSON格式,并过滤掉非JSON格式的数据
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });

        DataStream<String> sideOutput = jsonObjDS.getSideOutput(outputTag);

        //TODO 4.使用状态编程做新老用户校验
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedByMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitDtState;

            // 状态
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                String isNew = value.getJSONObject("common").getString("is_new");
                String lastVisitDt = lastVisitDtState.value();
                Long ts = value.getLong("ts");
                //2.判断是否为"1"
                if ("1".equals(isNew)) {
                    String curDt = DateFormatUtil.toDate(ts);

                    if (lastVisitDt == null) {
                        lastVisitDtState.update(curDt);
                    } else if (!lastVisitDt.equals(curDt)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastVisitDt == null) {
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    lastVisitDtState.update(yesterday);
                }

                return value;
            }
        });

        //TODO 5.使用侧输出流对数据进行分流处理
        // 页面浏览: 主流
        // 启动日志：侧输出流
        // 曝光日志：侧输出流
        // 动作日志：侧输出流
        // 错误日志：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String jsonString = value.toJSONString();

                String error = value.getString("err");
                if (error != null) {
                    ctx.output(errorTag, jsonString);
                }

                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startTag, jsonString);
                } else {
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    String common = value.getString("common");

                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            display.put("common", common);

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page_id", pageId);
                            action.put("ts", ts);
                            action.put("common", common);

                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    //输出数据到页面浏览日志
                    /**
                     * value这种写法存在风险，阿里的json内层使用的是map，hashmap，对hashmap前加后删操作，会抛异常，自己百度，此处不说明
                     */
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());

                }

            }
        });

        //TODO 6.提取各个数据的数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        //TODO 7.将各个流的数据分别写出到Kafka对应的主题中
        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic));
        errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic));

        //TODO 8.启动
        env.execute("BaseLogApp");


    }
}
