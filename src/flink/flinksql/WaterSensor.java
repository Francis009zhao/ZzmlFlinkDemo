package com.zzml.flinklearn.sql.atguigu;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:WaterSensor
 * @Auther: zzml
 * @Description:
 * @Date: 2022/8/30 22:50
 * @Version: v1.0
 * @ModifyDate:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    public String id;

    public Long ts;

    public int vc;

}
