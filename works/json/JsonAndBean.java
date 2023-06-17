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
        // 方法一: bean转成json的方法使用的toJSON方法，将bean对象传入即可
        JSONObject jsonStu = (JSONObject) JSONObject.toJSON(studentBean);
        System.out.println(jsonStu.toJSONString());

        // 方法二: 使用toJSONString() 来将bean对象传入，即可将bean对象转换为json
        String jsonStr = JSON.toJSONString(studentBean);
        System.out.println(">>>" + jsonStr);

    }

    /**
     * json 转成 Bean
     */
    public static void jsonToBean(String str){

        // 使用parseObject方法，将json格式的数据传入，并指定bean结构的class
        StudentBean studentBean = JSON.parseObject(str, StudentBean.class);

        // JSONObject 继承 JSON，因此也同样可以调用 parseObject 方法来将json转换成bean
        StudentBean stuJson = JSONObject.parseObject(str, StudentBean.class);

        System.out.println(studentBean);

    }

    /**
     * json格式的String类型字符串转成JsonObject
     */
    public static void jsonStringToJsonObject(String str){

        // JSONObject调用parseObject，将json格式的String类型数据传入，即可转换成JSONObject
        JSONObject jsonObject = JSONObject.parseObject(str);
        System.out.println(jsonObject);

    }

}
