package com.zzml.flinklearn.sql.atguigu;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSqlProcessTimeStreamToTable
 * @Auther: zzml
 * @Description:
 * @Date: 2022/9/1 22:29
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSqlProcessTimeStreamToTable {

    public static void main(String[] args) throws Exception {

        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 版本不对，下面会报错
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
//
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        TableEnvironment.create(env)

        //E:\FlinkDemo\src\main\java\com\zzml\flinklearn\data
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.readTextFile("E:\\FlinkDemo\\src\\main\\java\\com\\zzml\\flinklearn\\data\\data.txt")
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

//        waterSensorDS.print();

        // 将流转换为表，并指定处理时间
//        Table table = tableEnv.fromDataStream(waterSensorDS,
//                $("id"),
//                $("ts"),
//                $("vc"),
//                $("pt").rowtime());

        // 打印元数据信息
//        table.printSchema();

        env.execute();

    }
}
