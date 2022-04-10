package cn._51doit.flink.day05;

import cn._51doit.flink.day05.func.GeoRichMapFunction;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class DimDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<LogBean> logBeanDataStream = lines.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String value) throws Exception {
                LogBean bean = null;
                try {
                    bean = JSON.parseObject(value, LogBean.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return bean;
            }
        });

        SingleOutputStreamOperator<LogBean> filtered = logBeanDataStream.filter(e -> e != null);

        //关联维度信息
        SingleOutputStreamOperator<LogBean> logBeanWithNameDataStream = filtered.map(new RichMapFunction<LogBean, LogBean>() {

            private transient Connection connection;
            private transient PreparedStatement prepareStatement;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=utf-8", "root", "123456");
                prepareStatement = connection.prepareStatement("select id, name from tb_category where id = ?");
            }

            @Override
            public LogBean map(LogBean value) throws Exception {
                prepareStatement.setInt(1, value.cid);
                ResultSet resultSet = prepareStatement.executeQuery();
                String name = null;
                if (resultSet.next()) {
                    name = resultSet.getString(2);
                }
                resultSet.close();
                value.name = name;
                return value;
            }

            @Override
            public void close() throws Exception {
                if (prepareStatement != null) {
                    prepareStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        });

        //查询经纬度，关联位置信息
        SingleOutputStreamOperator<LogBean> result = logBeanWithNameDataStream.map(new GeoRichMapFunction("4924f7ef5c86a278f5500851541cdcff"));

        result.print();

        env.execute();
    }
}
