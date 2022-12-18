package com.zzml.flinklearn.exer.topn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:InterviewDemoAppCount
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/27 01:26
 * @Version: v1.0
 * @ModifyDate:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InterviewDemoAppCountBean {

    public String appId;

    public Long cnt;

    public String ts;

}
