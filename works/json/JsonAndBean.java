package com.zzml.flinklearn.works.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:JsonToBean
 * @Auther: zzml
 * @Description:
 * @Date: 2023/6/14 23:09
 * @Version: v1.0
 * @ModifyDate:
 */

public class JsonAndBean {

    public static void main(String[] args) {

        StudentBean studentBean = new StudentBean();
        studentBean.setName("Flink");
        studentBean.setAge("10");
        studentBean.setAddress("杭州");

        beanToJson(studentBean);

        String str = "{\"name\":\"FlinkAI\",\"age\":18}";
        jsonToBean(str);

    }

    /**
     * Bean 转成 json
     */
    public static void beanToJson(StudentBean studentBean){
        JSONObject jsonStu = (JSONObject) JSONObject.toJSON(studentBean);
        System.out.println(jsonStu.toJSONString());

    }

    /**
     * json 转成 Bean
     */
    public static void jsonToBean(String str){

        StudentBean studentBean = JSON.parseObject(str, StudentBean.class);

        System.out.println(studentBean);

    }

}
