package com.zzml.flinklearn.sql.atguigu;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:StreamToTableAgg
 * @Auther: zzml
 * @Description:
 * @Date: 2022/8/30 23:45
 * @Version: v1.0
 * @ModifyDate:
 */

public class StreamToTableAgg {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop100", 8888)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 使用tableAPI过滤出ws_001的数据
        // select id,sum(vc) from sensor where vc>=20 group by id;
        Table selectTable = sensorTable.where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

//        DataStream<Row> resultDS = tableEnv.toAppendStream(selectTable, Row.class);
        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(selectTable, Row.class);

//        tableEnv.toDataStream()

        resultDS.print();

        env.execute();

    }
}
