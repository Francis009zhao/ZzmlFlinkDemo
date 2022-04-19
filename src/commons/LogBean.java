package cn._51doit.flink.day05;

public class LogBean {

    public String oid;

    public Integer cid;

    public Double money;

    public Double longitude;

    public Double latitude;

    public String name;

    public String province;

    public String city;

    @Override
    public String toString() {
        return "LogBean{" +
                "oid='" + oid + '\'' +
                ", cid=" + cid +
                ", money=" + money +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", name='" + name + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
