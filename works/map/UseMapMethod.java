package com.zzml.flinklearn.works.map;


import com.google.common.collect.Maps;

import java.util.*;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:UseMapMethod
 * @Auther: zzml
 * @Description: map的常用方法
 * @Date: 2023/6/18 21:36
 * @Version: v1.0
 * @ModifyDate:
 */

public class UseMapMethod {

    public static void main(String[] args) {

        HashMap<Integer, String> tecMap = new HashMap<>();

        tecMap.put(10, "Flink");
        tecMap.put(2, "Kafka");
        tecMap.put(3, "Hbase");
        tecMap.put(4, "ClickHouse");
        tecMap.put(5, "Doris");
        tecMap.put(6, "Redis");
        tecMap.put(7, "Java");
        tecMap.put(8, "Python");
        tecMap.put(9, "SQL");

        System.out.println("sortByValue:" + sortByValue(tecMap, true));

        mapToList(tecMap);

        readMap(tecMap);

        // 将map的key转为list
        List<Integer> keyList = new ArrayList<>(tecMap.keySet());

        System.out.println(keyList);

        // 将map的value转为list
        List<String> valueList = new ArrayList<>(tecMap.values());
        System.out.println(valueList);

        // 将map的键值对转为list
        ArrayList<Map.Entry<Integer, String>> entries = new ArrayList<>(tecMap.entrySet());
        System.out.println(entries);

        for (Map.Entry<Integer, String> entry : entries) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        Iterator<Map.Entry<Integer, String>> iterator = entries.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }


        tecMap.forEach((k, v) -> {
            System.out.println(k + tecMap.get(k));
        });

        System.out.println("-------------------------------------");



    }

    /**
     * map转换成list
     * @param tecMap: 输入的map
     */
    public static void mapToList(HashMap<Integer, String> tecMap){

        // 通过ArrayList将map的数据转成list即可
        List<Map.Entry<Integer, String>> arrayList = new ArrayList<>(tecMap.entrySet());

        System.out.println(arrayList);

    }

    /**
     * 遍历map的几种方法
     * @param tecMap: input map data
     */
    public static void readMap(HashMap<Integer, String> tecMap){

        System.out.println("-------------map的循环方式-------------");

        Set<Map.Entry<Integer, String>> entries = tecMap.entrySet();

        System.out.println("for方式遍历map");
        // for方式循环
        for (Map.Entry<Integer, String> entry : entries) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        System.out.println("iterator方式遍历map");
        Iterator<Map.Entry<Integer, String>> it = entries.iterator();
        while (it.hasNext()){
            Map.Entry<Integer, String> next = it.next();
            System.out.println(next.getKey() + ":" + next.getValue());
        }

        // for方式遍历map的key和value
        for (Integer key : tecMap.keySet()) {
            System.out.println(key);
        }

        for (String value : tecMap.values()){
            System.out.println(value);
        }

        System.out.println("forEach方式遍历map");
        tecMap.forEach((k, v) -> {
            System.out.println(k + ":" + v);
        });


    }

    /**
     * 根据map的key进行排序，并输出map结果。
     * <p>1.先将map的key取出转成list</p>
     * <p>2.使用Collections工具类的sort方法对map的可以进行排序即可</p>
     */
    public static void sortedByKey(Map<Integer, String> tecMap){

        HashMap<Integer, String> resultMap = new HashMap<>();
        ArrayList<Integer> keyList = new ArrayList<>(tecMap.keySet());

        Collections.sort(keyList);

        for (int i = 0; i< keyList.size(); i++){
            resultMap.put(keyList.get(i), tecMap.get(keyList.get(i)));
        }

        System.out.println("排序结果" + resultMap);

    }

    /**
     * map按value排序的方法。
     * @param map : 输入的数据
     * @param isDesc : 是否是降序
     * @param <K> : key的类型
     * @param <V> : value的类型
     * @return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean isDesc){
        Map<K, V> result = Maps.newLinkedHashMap();

        // 降序
        if (isDesc){
            map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByValue().reversed())
                    .forEachOrdered(e -> result.put(
                            e.getKey(), e.getValue()
                    ));
        }else {  // 升序
            map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByValue())
                    .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        }

        return result;

    }


    /**
     * 直接使用TreeMap，treeMap是有序； 该方式是否存在问题，可以进一步探索，待探查
     */
    public static void sortTreeMap(HashMap<Integer, String> tecMap){
        TreeMap<Integer, String> sortMap = new TreeMap<>(tecMap);
        System.out.println(sortMap);
    }




}
