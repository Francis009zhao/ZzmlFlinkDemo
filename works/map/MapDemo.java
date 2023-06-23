package com.zzml.flinklearn.works.map;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:MapDemo
 * @Auther: zzml
 * @Description:
 * @Date: 2023/6/18 21:01
 * @Version: v1.0
 * @ModifyDate:
 */

public class MapDemo {

    /**
     * 说明：
     * <p>1.map添加数据的方式是put</p>
     * <p>2.put添加数据时如果key相同，则新值覆盖原先的值</p>
     * <p>3.Map的泛型一般是<String, Object>来表示键值对的类型，String为key的类型，Object为value的类型</></p>
     * <p>4.使用Object来作为value的类型，可以涵盖所有可能</p>
     * <p>5.Iterator泛型为<Map.Entry>，Map.Entry表示Map里的键值对，而Map.Entry的泛型为<String,Object>，所以Iterator最终的泛型表达为<Map.Entry<String,Object>></p>
     * @param args
     */
    public static void main(String[] args) {

        // 初始化集合set, key的类型我们直接定义为String类型，而value定义为object类型，
        HashMap<String, Object> hashMap = new HashMap<>();

        // 通过put往map添加<k,v>格式数据
        hashMap.put("Flink", "flink");
        hashMap.put("Hbase", "hbase");
        hashMap.put("Kafka", "kafka");

        // 获取map的长度
        System.out.println(hashMap.size());

        // 查询某个key的value
        System.out.println("key = Hbase 的value = " + hashMap.get("Hbase"));

        // 查询key是否存在, containsKey是全匹配，区分大小写
        System.out.println("map中是否存在Flin：" + hashMap.containsKey("Flin"));
        System.out.println("map中是否存在Flink：" + hashMap.containsKey("Flink"));
        System.out.println("map中是否存在flink：" + hashMap.containsKey("flink"));

        // 查询value是否存在, containsValue是全匹配，区分大小写
        System.out.println("map中是否存在flin：" + hashMap.containsValue("flin"));
        System.out.println("map中是否存在flink：" + hashMap.containsValue("flink"));
        System.out.println("map中是否存在Flink：" + hashMap.containsValue("Flink"));

        // 删除指定的键值对
        System.out.println("删除key为Hbase的键值对——> " + hashMap.get("Hbase") + ":" + hashMap.remove("Hbase"));

        hashMap.put("Redis", 6.0);
        hashMap.put("Mysql", 5.7);
        hashMap.put("doris", "starRocks");

        // for循环遍历map, 通过keySet方法来获取所有的key
        for (String key : hashMap.keySet()) {
            System.out.print(key + ":" + hashMap.get(key) + ",");
        }

        System.out.println();

        /**
         * 利用forEach循环遍历Map
         * <p>1.对于集合，foreach 循环实际上是用的 iterator 迭代器迭代，写法也一样</p>
         * <p>2.对于数组，foreach 循环实际上还是用的普通的 for 循环，怎么说foreach 循环就是for 循环</p>
          */
        hashMap.forEach((key, value) -> {
            System.out.print(key + ":" + hashMap.get(key) + ",");
        });

        System.out.println();
        System.out.println(">>>>>>>>>>>>>>>>");
        // 迭代器iterator遍历map
        Iterator<Map.Entry<String, Object>> iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> entry = iterator.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }


    }

}
