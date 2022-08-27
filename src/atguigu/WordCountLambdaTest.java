package com.zzml.flinklearn.doitedu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:WordCountLambdaTest
 * @Auther: zzml
 * @Description:
 * @Date: 2022/7/3 23:40
 * @Version: v1.0
 * @ModifyDate:
 */

public class WordCountLambdaTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("hadoop100", 8888);

        /**
         * lambda表达式怎么写，看你要实现的那个接口的方法接收什么参数，返回什么结果
         */
        // 然后就按lambda语法来表达：  (参数1,参数2,...) -> { 函数体 }
        // streamSource.map( (value) -> { return  value.toUpperCase();});

        // 由于上面的lambda表达式，参数列表只有一个，且函数体只有一行代码，则可以简化
        // streamSource.map( value ->  value.toUpperCase() ) ;
        lines.map(line -> line.toLowerCase());
        // 由于上面的lambda表达式， 函数体只有一行代码，且参数只使用了一次，可以把函数调用转成  “方法引用”
        SingleOutputStreamOperator<String> upperCased = lines.map(String::toUpperCase);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = upperCased.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        })
                //.returns(new TypeHint<Tuple2<String, Integer>>() {
                //});// 通过 TypeHint 传达返回数据类型
        //.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        //})) // 更通用的，是传入TypeInformation,上面的TypeHint也是封装了TypeInformation
        .returns(Types.TUPLE(Types.STRING, Types.INT));  // 利用工具类Types的各种静态方法，来生成TypeInformation

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = wordAndOne.keyBy(tp -> tp.f0);

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream1 = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        tuple2StringKeyedStream1.sum(1).print();

        env.execute();


    }

}
