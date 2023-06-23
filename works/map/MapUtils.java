package com.zzml.flinklearn.works.map;

import com.google.common.collect.Maps;
import com.zzml.flinklearn.works.bean.Person;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:MapUtils
 * @Auther: zzml
 * @Description: map常用工具类
 * @Date: 2023/6/23 12:33
 * @Version: v1.0
 * @ModifyDate:
 */

public class MapUtils {

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

        System.out.println(sortByValue(tecMap, true));
        System.out.println(sortByKey(tecMap, true));

        sortByBean();

    }

    /**
     * 按value排序，
     *
     * @param map    : 输入的map数据
     * @param isDesc : true：降序； false：升序
     * @param <K>    : key的类型
     * @param <V>    : value的类型
     * @return 返回map
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean isDesc) {

        // 使用newLinkedHashMap来获取结果
        Map<K, V> result = Maps.newLinkedHashMap();

        // 按value降序
        if (isDesc) {
            map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByValue().reversed())
                    .forEach(e -> result.put(e.getKey(), e.getValue()));
        } else {
            map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByValue())
                    .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        }

        /**
         // 使用Collections 来排序
         ArrayList<Map.Entry<K, V>> listMap = new ArrayList<>(map.entrySet());

         if (isDesc) {
         listMap.sort(new Comparator<Map.Entry<K, V>>() {
        @Override public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        int compare = (o1.getValue()).compareTo(o2.getValue());
        return -compare;
        }
        });
         }else {
         listMap.sort(new Comparator<Map.Entry<K, V>>() {
        @Override public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o1.getValue()).compareTo(o2.getValue());
        }
        });
         }

         for (Map.Entry<K, V> entry : listMap) {
         result.put(entry.getKey(), entry.getValue())
         }
         */
        return result;

    }

    /**
     * 按key排序，
     *
     * @param map    : 输入的map数据
     * @param isDesc : true：降序； false：升序
     * @param <K>    : key的类型
     * @param <V>    : value的类型
     * @return 返回map
     */
    public static <K extends Comparable<? super K>, V> Map<K, V> sortByKey(Map<K, V> map, boolean isDesc) {

        // 使用newLinkedHashMap来获取结果
        Map<K, V> result = Maps.newLinkedHashMap();

        // 按value降序
        if (isDesc) {
            map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByKey().reversed())
                    .forEach(e -> result.put(e.getKey(), e.getValue()));
        } else {
            map.entrySet()
                    .stream()
                    .sorted(Map.Entry.<K, V>comparingByKey())
                    .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        }

        return result;

    }

    public static void sortByBean() {

        List<Person> personList = new ArrayList<>();

        personList.add(new Person("Flink", "man",10, 550000, "杭州", "18956963663"));
        personList.add(new Person("Hbase", "f",9, 250000, "杭州", "18956963663"));
        personList.add(new Person("Redis", "f",7, 250000, "杭州", "18956963663"));
        personList.add(new Person("Kafka", "man",3, 350000, "杭州", "18956963663"));
        personList.add(new Person("Doris", "man",2, 280000, "杭州", "18956963663"));

//        System.out.println(personList);

        // 按工资升序排序, 并将结果输出
        List<Person> resultSalaryAsc = personList.stream()
                .sorted(Comparator.comparing(Person::getSalary))
                .collect(Collectors.toList());
        // 按工资升序排序，将排序结果的userName输出
        List<String> resultPersonUserName = personList.stream()
                .sorted(Comparator.comparing(Person::getSalary))
                .map(Person::getUserName)
                .collect(Collectors.toList());

        // 使用map、flatMap、filter等选择要输出的信息
        List<Tuple3<String, String, Double>> resultTuple3 = personList.stream()
                .sorted(Comparator.comparing(Person::getSalary))
                .map(new Function<Person, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> apply(Person person) {
                        return new Tuple3<>(person.getUserName(), person.getAddress(), person.getSalary());
                    }
                })
                .collect(Collectors.toList());

        // 按工资降序排序，并将结果输出
        List<Person> resultSalaryDesc = personList.stream()
                .sorted(Comparator.comparing(Person::getSalary).reversed())
                .collect(Collectors.toList());

        // 先按工资再年龄升序
        List<Person> resultSalaryAgeAsc = personList.stream()
                .sorted(Comparator.comparing(Person::getSalary).thenComparing(Person::getAge))
//                .sorted(Comparator.comparing(Person::getSalary).reversed().thenComparing(Person::getAge))
                .collect(Collectors.toList());

        // 先按工资再按年龄自定义排序
        List<Person> resultCollect = personList.stream()
                .sorted((p1, p2) -> {
                    if (p1.getSalary() == p2.getSalary()) {
                        return p2.getAge() - p1.getAge();
                    } else {
                        return (int)(p2.getSalary() - p1.getSalary());
                    }
                }).collect(Collectors.toList());

        System.out.println("按工资升序排序: \n" + resultSalaryAsc);
        System.out.println("只取userName信息: \n" + resultPersonUserName);
        System.out.println("map输出的信息: \n" + resultTuple3);
        System.out.println("按工资降序: \n" + resultSalaryDesc);
        System.out.println("先按工资再按年龄升序: \n" + resultSalaryAgeAsc);
        System.out.println(resultCollect);


    }


}
