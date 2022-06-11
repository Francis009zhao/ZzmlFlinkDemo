package com.zzml.flink.statebackend;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zzml.flink.beans.ods.HouseholdAppliancesBean;
import com.zzml.flink.commons.FlinkProjectConstant;
import com.zzml.flink.connectors.KafkaConnectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


/**
 * DEC:
 * 1.用于测试从kafka消费数据，并将结果输出到kafka的hdfs的stateBackend启动，和从savePoint启动的程序。
 * 2.使用hdfs存储stateBackend
 * <p>
 * Author: zzml
 * Date: 2022-05-03
 * <p>
 * Logic Thinking:
 * 1. kafka——>flink——>redis
 * 2. 启动集群相关： 启动kafka生产者造数，
 * 3. 编写ck的相关参数配置
 * 4. 设置重启策略为固定延迟重启
 * 5. 添加source，并进行transformation（json、map、keyBy、sum）等操作
 * 6. sink到redis
 *
 * 启动输入的参数：--set.kafka.topic house-hold-202205 --set.kafka.group.id house20220603 --set.kafka.offset earliest --hdfs.stateBackend hdfs://hadoop100:8020/flink/ck
 */

public class FlinkStateBackendJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkStateBackendJob.class);

    public static void main(String[] args) throws Exception {

        // 将配置的参数输出,输出暂时失败
        // Arrays.stream(args).forEach(arg -> log.info("{}", arg));

        // 获取配置参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * checkpoint相关设置
         */
        // 设置状态存储的后端,parameterTool.getRequired()当传入的值为null时报错，--hdfs.stateBackend hdfs://hadoop100:8020/flink/ck
        env.setStateBackend(new FsStateBackend(parameterTool.getRequired("hdfs.stateBackend")));

        // --chkInterval 30000L
        // 设置Checkpoint的时间间隔为30s做一次Checkpoint/其实就是每隔30s发一次Barrier!
        env.enableCheckpointing(parameterTool.getLong("chkInterval", 30000L));

        // 设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


        // 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        // env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
        // env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);  //默认值为0，表示不容忍任何检查点失败

        // 如果手动cancel job后，不删除job保存在hdfs上的ck数据
        // 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        //env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1

        //=============重启策略===========
        //-1.默认策略:配置了Checkpoint而没有配置重启策略默认使用无限重启
        //-2.配置无重启策略
        //env.setRestartStrategy(RestartStrategies.noRestart());
        //-3.固定延迟重启策略--开发中使用!
        //重启3次,每次间隔10s
        /*env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, //尝试重启3次
                Time.of(10, TimeUnit.SECONDS))//每次重启间隔10s
        );*/
        //-4.失败率重启--偶尔使用
        //5分钟内重启3次(第3次不包括,也就是最多重启2次),每次间隔10s
        /*env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // 每个测量时间间隔最大失败次数
                Time.of(5, TimeUnit.MINUTES), //失败率测量的时间间隔
                Time.of(10, TimeUnit.SECONDS) // 每次重启的时间间隔
        ));*/

        //上面的能看懂就行,开发中使用下面的代码即可
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

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
