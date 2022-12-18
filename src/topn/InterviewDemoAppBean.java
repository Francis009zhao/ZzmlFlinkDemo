package com.zzml.flinklearn.exer.topn;

import com.zzml.flink.beans.AllBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:InterviewDemoAppBean
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/27 00:53
 * @Version: v1.0
 * @ModifyDate:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InterviewDemoAppBean {

    public String appId;

    public String appName;

    public String eventTs;


}
