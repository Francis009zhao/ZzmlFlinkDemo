package com.zzml.flinklearn.works.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:Person
 * @Auther: zzml
 * @Description:
 * @Date: 2023/6/23 13:18
 * @Version: v1.0
 * @ModifyDate:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {

    private String userName;

    private String sex;

    private int age;

    private double salary;

    private String address;

    private String iphone;

}
