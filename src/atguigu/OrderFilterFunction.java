package com.zzml.flinklearn.gmall.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zzml.flinklearn.gmall.common.GmallConfig;
import com.zzml.flinklearn.gmall.utils.JdbcUtil;
import com.zzml.flinklearn.gmall.utils.JedisUtil;
import com.zzml.flinklearn.gmall.utils.MyKafkaUtil;
import com.zzml.flinklearn.gmall.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:OrderFilterFunction
 * @Auther: zzml
 * @Description:
 * @Date: 2022/6/30 22:29
 * @Version: v1.0
 * @ModifyDate:
 */

public class OrderFilterFunction {

    // 关联得到维度数据
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        // 拼接key
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoStr = jedis.get(redisKey);

        if (dimInfoStr != null){
            jedis.expire(redisKey, 600);

            jedis.close();

            return JSONObject.parseObject(dimInfoStr);
        }

        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        JSONObject dimInfo = queryList.get(0);

        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 600);
        jedis.close();


        return dimInfo;

    }


    public static SingleOutputStreamOperator<JSONObject> getDwdOrderDetail(StreamExecutionEnvironment env, String groupId) {

        final String topic = "";
        DataStreamSource<String> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = orderDetailDS.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                if (!"".equals(value)) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(value);
                        if ("insert".equals(jsonObject.getString("type"))) {
                            out.collect(jsonObject);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        });

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        SingleOutputStreamOperator<JSONObject> processResult = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("order_detail", JSONObject.class);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject orderDetail = valueState.value();

                if (orderDetail == null) {
                    valueState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
                } else {
                    String sateTs = orderDetail.getString("ts");
                    String curTs = value.getString("ts");

                    int compare = TimestampLtz3CompareUtil.compare(sateTs, curTs);
                    if (compare != 1) {
                        valueState.update(value);
                    }
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject orderDetail = valueState.value();
                out.collect(orderDetail);
            }
        });

        return processResult;


    }

}
