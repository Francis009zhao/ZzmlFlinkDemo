package com.zzml.flink.statebackend;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zzml.flink.beans.CheckPointParamBean;
import com.zzml.flink.beans.ods.HouseholdAppliancesBean;
import com.zzml.flink.commons.CheckpointParams;
import com.zzml.flink.commons.FlinkCheckpoint;
import com.zzml.flink.commons.FlinkProjectConstant;
import com.zzml.flink.connectors.KafkaConnectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:TestStateBackendJob
 * @Auther: zzml
 * @Description: 测试ck封装的代码
 * @Date: 2022/6/4 17:52
 * @Version: v1.0
 * @ModifyDate:
 */

public class TestStateBackendJob {


    private static final Logger log = LoggerFactory.getLogger(TestStateBackendJob.class);

    public static void main(String[] args) throws Exception {

        // 将配置的参数输出,输出暂时失败
        // Arrays.stream(args).forEach(arg -> log.info("{}", arg));

        // 获取配置参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //上面的能看懂就行,开发中使用下面的代码即可
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        CheckPointParamBean checkPointParamBean = CheckpointParams.buildCheckpointParam(parameterTool);

        FlinkCheckpoint.setCheckpoint(env, checkPointParamBean);

        // kafka相关参数
        // --set.kafka.topic topic_name
        final String topic = parameterTool.get(FlinkProjectConstant.SET_KAFKA_TOPIC);
        // --set.kafka.group.id groupId
        final String groupId = parameterTool.get(FlinkProjectConstant.SET_KAFKA_GROUP_ID);
        // --set.kafka.offset earliest
        final String offset = parameterTool.get(FlinkProjectConstant.SET_KAFKA_OFFSET);

        /**
         * 没有开启checkpoint，让flink提交偏移量的消费者定期自动提交偏移量，这个任务有jobManager负责处理
         * 下面记录偏移量的方式，不可靠，可能存在数据消费完成后，但任务没有记录偏移量，所以我们可以将偏移量存储到
         * hdfs上的stateBackEnd，如果任务重启，可以通过外部系统读取偏移量来恢复数据。
         */

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaConnectors.getKafkaConsumer(topic, groupId, offset);

        // 设置在checkpoint是不将偏移量保存到kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);

        // 将数据流中的数据转换成json格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(JSON::parseObject);

        // 将数据转成json格式写法2
//        OutputTag<String> outputTag = new OutputTag<String>("outSide") {
//        };
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
//            @Override
//            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
//                try {
//                    JSONObject jsonObject = JSON.parseObject(value);
//                    out.collect(jsonObject);
//                } catch (Exception e) {
//                    ctx.output(outputTag, value);
//                }
//            }
//        });

        // 将数据映射到javabean
        SingleOutputStreamOperator<HouseholdAppliancesBean> houseAppDS = jsonObjDS.map(new MapFunction<JSONObject, HouseholdAppliancesBean>() {
            @Override
            public HouseholdAppliancesBean map(JSONObject value) throws Exception {
                return new HouseholdAppliancesBean(
                        value.getString("provinces"),
                        value.getString("orderId"),
                        value.getString("brandName"),
                        value.getString("productName"),
                        value.getDouble("price"),
                        value.getLong("orderTime"),
                        value.getLong("payTime")
                );
            }
        });

        // keyed操作
        KeyedStream<HouseholdAppliancesBean, String> keyedStream = houseAppDS.keyBy(HouseholdAppliancesBean::getProvinces);

        // sum操作
        SingleOutputStreamOperator<HouseholdAppliancesBean> summed = keyedStream.sum("price");

        summed.print();


        env.execute("FlinkStateBackendJob");
    }

}
