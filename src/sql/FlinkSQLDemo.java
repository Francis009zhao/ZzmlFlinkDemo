package com.zzml.flinklearn.sql.doitedu;

import com.alibaba.fastjson.JSONObject;
import com.zzml.flinklearn.doitedu.EventBean;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSQLDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2023/3/6 23:10
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSQLDemo {

    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建表对象
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 接入数据源
        DataStreamSource<String> urlDS = env.socketTextStream("", 9999);
        SingleOutputStreamOperator<EventBean> eventBeanDS = urlDS.map(url -> JSONObject.parseObject(url, EventBean.class));

        tEnv.createTemporaryView("event_table", eventBeanDS);

        tEnv.executeSql("select * from event_table").print();

        env.execute();

    }

}
