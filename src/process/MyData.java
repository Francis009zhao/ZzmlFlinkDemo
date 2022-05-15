package com.zzml.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyData {
    public int keyId;
    public long timestamp;
    public double value;
}
