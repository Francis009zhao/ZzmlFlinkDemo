package com.zzml.flink.source;

import com.zzml.flink.bean.ods.HouseholdAppliancesBean;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import java.util.Random;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:UserDefineDataStreamDataSource
 * @Auther: zzml
 * @Description: 自定义数据生成器，将产生的数据输出到kafka的Topic中, 数据生成器模板
 * @Date: 2022/5/21 20:49
 * @Version: v1.0
 * @ModifyDate:
 */

public class UserDefineDataStreamDataSource {

    public static void main(String[] args) throws Exception {

        // 1.创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);


        /**
         * 传入pojo类，使用RandomGenerator生成pojo类对应结构的数据
         */
        DataGeneratorSource<HouseholdAppliancesBean> dataGeneratorSource = new DataGeneratorSource<>(
                // RandomGenerator 可以生成随机数据
                new RandomGenerator<HouseholdAppliancesBean>() {
                    @Override
                    public HouseholdAppliancesBean next() {

                        String[] brand = {"格力", "Haier", "美的", "松下", "TCL","老板电器"};
                        String[] products = {"空调", "冰箱", "电饭煲", "电磁炉", "热水器","洗衣机"};
                        String[] provinces = {"河北省", "山西省", "辽宁省", "吉林省", "黑龙江省", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省", "湖北省", "湖南省", "广东省", "海南省", "四川省", "贵州省", "云南省", "陕西省", "甘肃省", "青海省", "台湾省", "北京市", "天津市", "上海市", "重庆市", "香港", "澳门", "内蒙", "宁夏", "广西", "西藏", "新疆"};

                        return new HouseholdAppliancesBean(
                                provinces[random.nextInt(0, 33)],
                                random.nextHexString(11),
                                brand[random.nextInt(0, 5)],
                                products[random.nextInt(0, 5)],
                                random.nextUniform(1000, 50000),
                                System.currentTimeMillis(),
                                System.currentTimeMillis() + 1000
                        );
                    }
                }
        );

        /**
         * 传入bean类，使用SequenceGenerator生成对应bean类对应的结构的数据
         * SequenceGenerator： 生成序列数据
         */


        SingleOutputStreamOperator<HouseholdAppliancesBean> producerData = env.addSource(dataGeneratorSource).returns(Types.POJO(HouseholdAppliancesBean.class));

        // 将数据输出到kafka
        producerData.print(">>>>>");

        env.execute("UserDefineDataStreamDataSource");


    }

}
