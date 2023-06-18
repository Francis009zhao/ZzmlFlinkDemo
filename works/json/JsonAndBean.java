package com.zzml.flinklearn.works.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        String str2 = "[{\"name\":\"FlinkAI\",\"age\":18}]";
        JsonAndBean jsonAndBean = new JsonAndBean();
        jsonAndBean.jsonToList(str2);

        jsonListToJsonArray();

        listToJsonArray();

        mapToJson();

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        jsonToMap();
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

    /**
     * json字符串转List
     */
    public void jsonToList(String str){

        List<Map> mapList = JSONArray.parseArray(str, Map.class);

        mapList.forEach(System.out::println);
    }

    /**
     * json 数组字符串转成jsonArray
     */
    public static void jsonListToJsonArray(){

        /**
         * 方法一：JSONArray.parseArray直接解析list格式的String字符串
         */
        String jsonList = "[{\"name\":\"FlinkAI\",\"age\":18}]";

        JSONArray parseArray = JSONArray.parseArray(jsonList);
        System.out.println(parseArray.toString());

        /**
         * 方法二：1.创建JSONArray对象，并往里面添加元素即可；
         *       2.JSONArray.toJSONString， 最终输出是String类型。
         */
        JSONArray jsonArray = new JSONArray();
        StudentBean studentBean = new StudentBean();
        studentBean.setName("flink");
        studentBean.setAddress("杭州市");
        studentBean.setAge("12");

        // 将bean对象添加到jsonArray中
        jsonArray.add(studentBean);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("Kafka", 3);

        jsonArray.add(jsonObject);

        String jsonString = JSONArray.toJSONString(jsonArray);

        System.out.println(jsonArray.getString(0));

        System.out.println(jsonString);

    }

    /**
     * List类型转成JsonArray
     */
    public static void listToJsonArray(){

        StudentBean studentBean = new StudentBean();
        List<StudentBean> list = new ArrayList<>();
        studentBean.setName("Flink");
        studentBean.setAge("13");
        studentBean.setAddress("杭州市");

        list.add(studentBean);

        String listJson = JSON.toJSONString(list);

//        JSONObject jsonObject = JSON.parseObject(listJson);

//        System.out.println(jsonObject.getString("name"));
//        System.out.println(jsonObject);
        System.out.println(listJson);

    }

    /**
     * Map 转成json
     */
    public static void mapToJson(){

        HashMap<String, String> maps = new HashMap<>();
        maps.put("tec", "FLink");
        maps.put("age", "15");
        maps.put("address", "杭州");

        String mapJson = JSON.toJSONString(maps);
        System.out.println(mapJson);

        System.out.println("maps>>>" + maps);

    }

    /**
     * jsonObject转成map
     */
    public static void jsonToMap(){

        StudentBean studentBean = new StudentBean();
        studentBean.setName("jsonToMap");
        studentBean.setAddress("深圳");

        String stuStr = JSONObject.toJSONString(studentBean);

        Map mapJson = JSONObject.parseObject(stuStr, Map.class);

        System.out.println(mapJson);

        System.out.println(mapJson.get("name"));

    }




}
