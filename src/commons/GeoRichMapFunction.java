package cn._51doit.flink.day05.func;

import cn._51doit.flink.day05.LogBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class GeoRichMapFunction extends RichMapFunction<LogBean, LogBean> {

    private String key;

    public GeoRichMapFunction(String key) {
        this.key = key;
    }

    private transient CloseableHttpClient httpclient;

    @Override
    public void open(Configuration parameters) throws Exception {
        httpclient = HttpClients.createDefault();
    }

    @Override
    public LogBean map(LogBean bean) throws Exception {
        double longitude = bean.longitude;
        double latitude = bean.latitude;
        HttpGet httpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?&location="+ longitude+"," +latitude+ "&key=" + key);
        CloseableHttpResponse response = httpclient.execute(httpGet);
        try {
            //System.out.println(response.getStatusLine)
            HttpEntity entity = response.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            String province = null;
            String city = null;
            if (response.getStatusLine().getStatusCode() == 200) {
                //获取请求的json字符串
                String result = EntityUtils.toString(entity);
                //转成json对象
                JSONObject jsonObj = JSON.parseObject(result);
                //获取位置信息
                JSONObject regeocode = jsonObj.getJSONObject("regeocode");
                if (regeocode != null && !regeocode.isEmpty()) {
                    JSONObject address = regeocode.getJSONObject("addressComponent");
                    //获取省市区
                    bean.province = address.getString("province");
                    bean.city = address.getString("city");
                }
            }
        } finally {
            response.close();
        }
        return bean;
    }

    @Override
    public void close() throws Exception {
        if (httpclient != null) {
            httpclient.close();
        }
    }
}
