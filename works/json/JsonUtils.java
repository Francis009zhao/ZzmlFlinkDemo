package com.zzml.flinklearn.works.json;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:JsonUtils
 * @Auther: zzml
 * @Description: json转换工具
 * @Date: 2023/6/18 13:35
 * @Version: v1.0
 * @ModifyDate:
 */

@Slf4j
public class JsonUtils {

    // 定义jackson对象
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     *
     * json数据转换成pojo对象list
     * <p>Desc: jsonToList</p>
     * @param <T> 返回的泛型
     * @return 返回数据
     */
    public static <T> List<T> jsonToList(String jsonStr, Class<T> beanType){

        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(List.class, beanType);

        try {
            return MAPPER.readValue(jsonStr, javaType);
        }catch (Exception e){
            log.error("Json Exception", e);
        }

        return null;

    }

}
