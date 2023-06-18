package com.zzml.flinklearn.works.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.*;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:JsonMethodDemo
 * @Auther: zzml
 * @Description: json方法练习
 * @Date: 2023/6/17 19:15
 * @Version: v1.0
 * @ModifyDate:
 */

public class JsonMethodDemo {

    /**
     * FastJson对于json格式字符串的解析主要用到了一下三个类:
     * 1.JSON：主要实现json对象，json对象数组，javabean，json字符串之间的相互转化。 转换后取值按各自的方法进行。
     * 2.JSONObject：fastJson提供的json对象。JSONObject是Map接口的实现类，可以直接用Map接口方法操作。
     * 3.JSONArray：fastJson提供JSON对象数组，通常是通过迭代器取得其中的JSONObject，再利用JSONObject的get()方法进行取值。
     * <p>
     * 常用方法：
     * 1 public static final Object parse(String text);// 把JSON文本转换成JSONObject或JSONArray
     * 2 public static final JSONObject parseObject(String text);// 把JSON文本转换成JSONObject
     * 3 public static final JSONArray parseArray(String text);// 把JSON文本转换成JSONArray
     * 4 public static final <T> T parseObject(String text, Class<T> clazz);// 把JSON文本转换成JavaBean
     * 5 public static final <T> List<T> parseArray(String text, Class<T> clazz); //把JSON文本转换成JavaBean集合
     * 6 public static final String toJSONString(Object object); // 将JavaBean序列化为JSON文本
     * 7 public static final String toJSONString(Object object, boolean prettyFormat); // 将JavaBean序列化为带格式的JSON文本
     * 8 public static final Object toJson(Obiect javaObject);//将JavaBean转换为JSONObject或者JSONArray
     */
    public static void main(String[] args) {

        readJsonObject();

        useJsonArray();

        String jsonStr = "{\"data\":[{\"msg\":\"成功\",\"status\":\"200\",\"data\":{\"records\":[{\"coder\":\"6d5\",\"name\":\"yuan\",\"type\":\"0001\"}],\"page_info\":{\"total\":1,\"page_no\":1,\"page_size\":1}}}],\"code\":\"200\",\"globalId\":\"df666\"}";

        JsonMethodDemo jsonMethodDemo = new JsonMethodDemo();
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        // 使用谷歌的工具类来创建，可以自动推断类型，本质与原生的java写法没什么区别
        ArrayList<JSONObject> jsonList = Lists.newArrayList();
        jsonList.add(jsonObject);
        ArrayList<HashMap<String, Object>> outputList = Lists.newArrayList();
        jsonMethodDemo.parseJson(jsonList, outputList);

//        System.out.println(outputList);

    }

    /**
     * 遍历jsonObject
     */
    public static void readJsonObject() {

        /**
         * 1. Set<String> keySet():获取JSONObject中的key，并将其放入Set集合中;
         * 2. Set<Map.Entry<String, Object>> entrySet():在循环遍历时使用，取得是键和值的映射关系，Entry就是Map接口中的内部接口
         */
        StudentBean studentBean = new StudentBean();
        studentBean.setName("Hbase");
        studentBean.setAddress("US");
        studentBean.setAge("10");

        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(studentBean);

        // 使用for循环来遍历jsonObject，
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

//        Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();
//
//        for (Map.Entry<String, Object> stringObjectEntry : entrySet) {
//            System.out.println(stringObjectEntry);
//        }


    }

    /**
     * 添加和遍历jsonArray
     */
    public static void useJsonArray() {

//        JSONArray jsonArray = new JSONArray();

        // 添加数据到jsonArray,string类型直接添加到jsonArray
        String strList = "[{\"name\":\"FlinkAI\",\"age\":18}]";

        JSONArray jsonArray = JSONArray.parseArray(strList);

        // 通过List进行添加
        StudentBean studentBean = new StudentBean();
        List<StudentBean> listStu = new ArrayList<>();
        studentBean.setName("Hbase");
        studentBean.setAddress("US");
        studentBean.setAge("10");

        listStu.add(studentBean);

        jsonArray.add(listStu);

        System.out.println(jsonArray);

        // for循环来遍历jsonArray
        for (int i = 0; i < jsonArray.size(); i++){

            if (jsonArray.get(i) instanceof List){
                String jsonArrayStr = JSON.toJSONString(jsonArray.get(i));
                JSONArray parseArray = JSONArray.parseArray(jsonArrayStr);
                JSONObject jsonObject = JSON.parseObject(parseArray.get(0).toString());
                System.out.println(jsonObject.getString("name"));
//                System.out.println("jsonArrayStr:" + jsonArrayStr);
            }else {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String name = jsonObject.getString("name");
                System.out.println(name);
            }
        }


    }

    /**
     * 递归读取json数据的方法
     *
     * @param jsonObject: 输入的json数据
     * @param inputMap:   存放json的key和value
     */
    private void recurseParseJson(JSONObject jsonObject, HashMap<String, Object> inputMap) {

        // 将json的key取出放入set集合
        Set<Map.Entry<String, Object>> jsonKeys = jsonObject.entrySet();

        // 循环判断value的类型，并进行对应的递归处理
        for (Map.Entry<String, Object> jsonKey : jsonKeys) {
            // 如果value类型为map，则继续递归，
            if (jsonKey.getValue() instanceof Map) {
                jsonObject = JSON.parseObject(JSON.toJSONString(jsonKey.getValue()));
                recurseParseJson(jsonObject, inputMap);
            } else if (jsonKey.getValue() instanceof List) {   // 如果value类型是List，则转成jsonArray，通过for循环取出jsonObject进行递归
                List<JSONObject> jsonList = JsonUtils.jsonToList(JSON.toJSONString(jsonKey.getValue()), JSONObject.class);
                for (JSONObject json : jsonList) {
                    recurseParseJson(json, inputMap);
                }
            } else {
                // 将普通的value存放到map中
                inputMap.put(jsonKey.getKey(), jsonKey.getValue());
            }

        }

    }

    /**
     * 当json格式的数据中还嵌套着json或jsonArray时的处理方法.此处我们使用递归方法。
     * 如：
     * {
     * "data": [{
     * "msg": "成功",
     * "status": "300",
     * "data": {
     * "records": [{
     * "coder": "6d5",
     * "name": "flink",
     * "type": "0001"
     * }],
     * "page_info": {
     * "total": 1,
     * "page_no": 1,
     * "page_size": 1
     * }
     * }
     * }],
     * "code": "200",
     * "globalId": "df666"
     * }
     * 遇到此类json数据，如果我们使用bean来参与解析，势必会存在一定程度的冗余，序列化和反序列化同样需要时间。
     * <p>
     * 我们可以使用下面方法： 通过JSONObject将复杂的JSON进行逐层解析，遇到数组的时候就用JSONArray拿下来，里面具体的数据再使用JSONObject获取
     * <p>
     * 具体步骤：
     * 1.先递归的将json格式的数据的key和value取出存放在map中；
     * 2.在通过parseJson方法将数据读取出来
     */
    public void parseJson(List<JSONObject> jsonList, List<HashMap<String, Object>> outputList) {

        for (int i = 0; i < jsonList.size(); i++){
            HashMap<String, Object> inputMap = Maps.newHashMap();
            JSONObject jsonObject = jsonList.get(i);

            // 判空处理，为null可以过滤掉
            if (jsonObject == null || jsonObject.isEmpty()){
                continue;
            }

            recurseParseJson(jsonObject, inputMap);

            // 将数据输出
            outputList.add(inputMap);

        }

    }

    /**
     * 递归合并成jsonObject
     * <p>Method: mergeJson</p>
     *
     */
    public void mergeJson(){

        // https://blog.csdn.net/cucgyfjklx/article/details/123920674?utm_medium=distribute.pc_relevant.none-task-blog-2~default~baidujs_baidulandingword~default-0-123920674-blog-128914958.235^v38^pc_relevant_sort_base2&spm=1001.2101.3001.4242.1&utm_relevant_index=3

    }




}
