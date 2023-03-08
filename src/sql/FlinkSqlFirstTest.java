package com.zzml.flinklearn.sql.doitedu;

import com.alibaba.fastjson.JSON;
import com.zzml.flinklearn.doitedu.EventBean;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSqlFirstTest
 * @Auther: zzml
 * @Description:
 * @Date: 2022/9/3 23:51
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSqlFirstTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> sourceDS = env.socketTextStream("hadoop100", 9999);
        SingleOutputStreamOperator<EventBean> eventDS = sourceDS.map(dt -> JSON.parseObject(dt, EventBean.class));

        /**
         * tableAPI
         */
        tEnv.createTemporaryView("t_event", eventDS);

        tEnv.executeSql("select * from t_event").print();

        /**
         * tableSQL
         */
        Table table = tEnv.fromDataStream(eventDS);

        Table result = table.groupBy($("guid"), $("sessionId"), $("eventId"))
                .aggregate($("eventId").count().as("event_cnt"))
                .select($("guid"), $("sessionId"), $("eventId"), $("event_cnt"));

        result.execute().print();

    }
}
