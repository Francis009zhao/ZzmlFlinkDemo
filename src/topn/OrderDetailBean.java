package com.zzml.flinklearn.exer.topn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:OrderDetailBean
 * @Auther: zzml
 * @Description: 订单表
 * @Date: 2022/11/9 11:54
 * @Version: v1.0
 * @ModifyDate:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetailBean {

    /**
     * 下单用户id
     */
    public Long userId;

    /**
     * 商品id
     */
    public Long itemId;

    /**
     * 城市名称
     */
    public String cityName;

    /**
     * 订单金额
     */
    public Double price;

    /**
     * 下单时间
     */
    public Long timeStamp;

}
