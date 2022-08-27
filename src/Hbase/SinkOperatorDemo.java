package com.zzml.flinklearn.doitedu;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Map;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:SinkOperatorDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/16 00:08
 * @Version: v1.0
 * @ModifyDate:
 */

public class SinkOperatorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        SingleOutputStreamOperator<Tuple5<String, Long, Map<String, String>, String, Long>> tpDS = streamSource.map(bean -> Tuple5.of(bean.getEventId(),
                bean.getGuid(),
                bean.getEventInfo(),
                bean.getSessionId(),
                bean.getTimeStamp())).returns(new TypeHint<Tuple5<String, Long, Map<String, String>, String, Long>>() {
        });

//        tpDS.print();
        // 输出到文件
//        tpDS.writeAsText("F:/datasets/flink_data", FileSystem.WriteMode.OVERWRITE);
//        tpDS.writeAsCsv("F:/datasets/flink_data/csv",FileSystem.WriteMode.OVERWRITE);

        /**
         * 应用  StreamFileSink 算子，来将数据输出到  文件系统
         */

        /**
         * 1. 输出为 行格式
         */
        // 构造一个FileSink对象
        FileSink<String> rowSink = FileSink.forRowFormat(new Path("F:/datasets/flink_data"), new SimpleStringEncoder<String>("utf-8"))
                // 文件的滚动策略 （间隔时长10s，或文件大小达到 5M，就进行文件切换
                .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(1000).withMaxPartSize(5 * 1024 * 1024).build())
                // 分桶的策略（划分子文件夹的策略）
                .withBucketAssigner(new DateTimeBucketAssigner<String>())
                // 输出文件的文件名相关配置
                .withBucketCheckInterval(5)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".txt").build())
                .build();

        // 然后添加到流，进行输出
        streamSource.map(JSON::toJSONString)
                //.addSink()  /* SinkFunction实现类对象,用addSink() 来添加*/
                .sinkTo(rowSink); /*Sink 的实现类对象,用 sinkTo()来添加  */


        /**
         * 2. 输出为 列格式
         *
         * 要构造一个列模式的 FileSink，需要一个ParquetAvroWriterFactory
         * 而获得ParquetAvroWriterFactory的方式是，利用一个工具类： ParquetAvroWriters
         * 这个工具类提供了3种方法，来为用户构造一个ParquetAvroWriterFactory
         *
         * ## 方法1：
         * writerFactory = ParquetAvroWriters.forGenericRecord(schema)
         *
         * ## 方法2：
         * writerFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLogBean.class)
         *
         * ## 方法3：
         * writerFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);
         *
         * 一句话：  3种方式需要传入的参数类型不同
         * 1. 需要传入schema对象
         * 2. 需要传入一种特定的JavaBean类class
         * 3. 需要传入一个普通的JavaBean类class
         *
         * 传入这些参数，有何用？
         * 这个工具 ParquetAvroWriters. 需要你提供你的数据模式schema（你要生成的parquet文件中数据模式schema）
         *
         * 上述的3种参数，都能让这个工具明白你所指定的数据模式（schema）
         *
         * 1. 传入Schema类对象，它本身就是parquet框架中用来表达数据模式的内生对象
         *
         * 2. 传入特定JavaBean类class，它就能通过调用传入的类上的特定方法，来获得Schema对象
         * （这种特定JavaBean类，不用开发人员自己去写，而是用avro的schema描述文件+代码生产插件，来自动生成）
         *
         * 3. 传入普通JavaBean,然后工具可以自己通过反射手段来获取用户的普通JavaBean中的包名、类名、字段名、字段类型等信息，来翻译成一个符合Avro要求的Schema
         *
         *
         */










        env.execute("SinkOperatorDemo");

    }
}
