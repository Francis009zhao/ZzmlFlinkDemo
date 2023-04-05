package com.zzml.flinklearn.sql.doitedu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.row;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:FlinkSqlTest
 * @Auther: zzml
 * @Description:
 * @Date: 2022/9/4 10:32
 * @Version: v1.0
 * @ModifyDate:
 */

public class FlinkSqlTest {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        DataType tableSchema = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("event", DataTypes.STRING()),
                DataTypes.FIELD("ts", DataTypes.BIGINT())
        );

        Table table = tEnv.fromValues(tableSchema,
                row(1, "e1", 1000),
                row(1, "e1", 2000),
                row(1, "e2", 3000),
                row(1, "e3", 4000),
                row(1, "e3", 5000),
                row(1, "e2", 6000),
                row(1, "e2", 7000)
        );

        tEnv.createTemporaryView("t", table);

        tEnv.executeSql("select event,count(1) as event_cnt from t group by event").print();

//        env.execute();

    }
}
