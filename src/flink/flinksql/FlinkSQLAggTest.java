package com.zzml.flinklearn.sql.atguigu;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSQLTest
 * @Auther: zzml
 * @Description:
 * @Date: 2022/8/31 22:43
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSQLAggTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取端口数据转换为javaBean
        DataStreamSource<String> line = env.socketTextStream("hadoop100", 8888);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = line.map(dt -> {
            String[] split = dt.split(",");
            return new WaterSensor(split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]));
        });

        // 将流进行表注册
        tableEnv.createTemporaryView("sensor", waterSensorDS);

        // 使用sql查询注册的表
        Table result = tableEnv.sqlQuery("select id,count(ts) as ct,sum(vc) as vc_sum from sensor where id = 'ws_001' group by id");

        // 将表对象转换为流进行打印输出
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();


    }
}
