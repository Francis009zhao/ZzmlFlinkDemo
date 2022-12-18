package com.zzml.flinklearn.exer.topn;

import java.sql.Timestamp;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:UrlViewCount
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/9 17:08
 * @Version: v1.0
 * @ModifyDate:
 */

public class UrlViewCount {

    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
