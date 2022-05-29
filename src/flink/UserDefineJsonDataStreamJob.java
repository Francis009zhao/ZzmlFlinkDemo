package com.zzml.flink.job.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zzml.flink.commons.CustomJsonKafkaSerializationSchema;
import com.zzml.flink.connectors.KafkaConnectors;
import com.zzml.flink.source.UserDefineDataStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:UserDefineJsonDataStreamJob
 * @Auther: zzml
 * @Description: 将数据转成json格式，并以json格式输出到kafka中
 * @Date: 2022/5/22 18:23
 * @Version: v1.0
 * @ModifyDate:
 */

public class UserDefineJsonDataStreamJob {

    public static void main(String[] args) throws Exception {

        /**
         * 获取args配置参数
         */
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // 参数传入方式为: --is.open.print true
        final boolean OPEN_PRINT = parameterTool.getBoolean("is.open.print", false);
        // 参数传入方式为: --output.topic topic_name
        final String OUTPUT_TOPIC = parameterTool.get("output.topic", "house-hold-202205");
        // 参数传入方式为: --side.output.topic side-output-data-stream
        final String SIDE_TOPIC = parameterTool.get("side.output.topic", "side-output-data-stream-all");

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStreamSource<String> dataStreamSource = env.addSource(new UserDefineDataStream());

        /**
         *  Transformation: 将每行数据转换成json格式
         */
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(JSON::parseObject);

        // 将每行数据转成json
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> processDS = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 将数据输出到侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });


        // sink
        if (OPEN_PRINT) {
            jsonObjDS.print("json>>>>>");
        }

        /**
         *  将数据sink到kafka，json格式
         */
        jsonObjDS.addSink(KafkaConnectors.getKafkaProducer(new CustomJsonKafkaSerializationSchema(OUTPUT_TOPIC)));
        // 另一种写法如下：实际上上面写法就是将下面写法封装成类
//        jsonObjDS.addSink(KafkaConnectors.getKafkaProducer(OUTPUT_TOPIC, new KafkaSerializationSchema<JSONObject>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
//                return new ProducerRecord<byte[], byte[]>(OUTPUT_TOPIC, element.toJSONString().getBytes());
//            }
//        }));



        // 侧输出流数据输出,数据没有输出，暂没解决问题
        //DataStream<String> sideOutputDS = jsonObjDS.getSideOutput(outputTag);
        //sideOutputDS.addSink(KafkaConnectors.getKafkaProducer(SIDE_TOPIC));


        // 执行
        env.execute("UserDefineJsonDataStreamJob");

    }


}
