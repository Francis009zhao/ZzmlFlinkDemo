package com.zzml.flinklearn.works.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:StudentBean
 * @Auther: zzml
 * @Description:
 * @Date: 2023/6/14 23:07
 * @Version: v1.0
 * @ModifyDate:
 */
@Data
//@AllArgsConstructor
//@NoArgsConstructor
public class StudentBean {

    private String name;

    private String age;

    private String address;

    @Override
    public String toString() {
        return "StudentBean{" +
                "name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
