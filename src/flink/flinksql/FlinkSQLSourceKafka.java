package com.zzml.flinklearn.sql.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSQLSourceKafka
 * @Auther: zzml
 * @Description:
 * @Date: 2022/8/31 22:37
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSQLSourceKafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用连接器的方式读取kafka的数据
//        tableEnv.from()

    }
}
