package com.zzml.flinklearn.atguigu.tableapi;

import com.zzml.flinklearn.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TestFirstTableApiGgg {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        DataStreamSource<String> dataStreamSource = env.socketTextStream("node01", 8888);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\FlinkDemo\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {

                String[] split = value.split(",");
                return new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        //使用tableAPI过滤数据
        Table selectTable = sensorTable.where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

        //将selectTable转换成流进行输出,toAppendStream是一个追加流，不支持更新
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(selectTable, Row.class);

//        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(selectTable, Row.class);

        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(selectTable);

        rowDataStream.print();

        env.execute();


    }


}
