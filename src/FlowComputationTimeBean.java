package com.zzml.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlowComputationTimeBean {
    private String appId;
    private String sign;
    private String detailId;
    private long eventTime;
    private String deviceId;
    private long timestamp;
    private long computationOperMapTime;
    private long computationExposureMapTime;
    private long computationOperFormatTime;
    private long computationExposureFormatTime;
    private long computationUnionTime;

}
