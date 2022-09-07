package com.zzml.flinklearn.sql.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSQLMySQL
 * @Auther: zzml
 * @Description:
 * @Date: 2022/9/1 00:01
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSQLMySQL {

    public static void main(String[] args) throws Exception {

        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册sourceTable
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with( "
                + "'connector'='kafka'," +
                "'topic'='source_topic'," +
                "'properties.bootstrap.servers'='hadoop100:9092,hadoop101:9092,hadoop102:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'startup.mode' = 'earliest-offset'," +
                "'format'='csv'" +
                ")");

        // 注册sinkTable：MySQL
        // sink_sensor:flink-mysql表；
        // sink_table：mysql本地表；
        tableEnv.executeSql("create table sink_sensor(is string, ts bigint, vc int) with(" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://localhost:3306/test'," +
                "'table-name' = 'sink_table'," +
                "'username'='root'," +
                "'password'='00000'" +
                ")");

        // 执行查询kafka数据写入mysql
        //Table source_sensor = tableEnv.from("source_sensor");

        //
        //source_sensor.executeInsert("sink_sensor");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='ws_001'");

        env.execute();

    }
}
