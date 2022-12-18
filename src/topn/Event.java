package com.zzml.flinklearn.exer.topn;

import java.sql.Timestamp;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:Event
 * @Auther: zzml
 * @Description:
 * @Date: 2022/11/9 17:06
 * @Version: v1.0
 * @ModifyDate:
 */

public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
