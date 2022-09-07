package com.zzml.flinklearn.sql.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSQLKafkaToKafka
 * @Auther: zzml
 * @Description:
 * @Date: 2022/8/31 23:06
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSQLKafkaToKafka {

    public static void main(String[] args) {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册sourceTable
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with( "
        +"'connector'='kafka'," +
                "'topic'='source_topic'," +
                "'properties.bootstrap.servers'='hadoop100:9092,hadoop101:9092,hadoop102:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'startup.mode' = 'earliest-offset'," +
                "'format'='csv'" +
                ")");

        // 注册sinkTable
        tableEnv.executeSql("create table sink_sensor (id string, ts bigint, vc int) with( "
                +"'connector'='kafka'," +
                "'topic'='sink_topic'," +
                "'properties.bootstrap.servers'='hadoop100:9092,hadoop101:9092,hadoop102:9092'," +
                "'format'='json'" +
                ")");

        // 执行查询插入数据
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='ws_001'");

    }
}
