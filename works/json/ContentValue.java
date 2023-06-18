package com.zzml.flinklearn.works.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:ContentValue
 * @Auther: zzml
 * @Description:
 * @Date: 2023/6/14 22:07
 * @Version: v1.0
 * @ModifyDate:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ContentValue {

    /**
     * {
     *     "formId": "{$formId}",
     *     "link": "www.java3y.com",
     *     "text": [{
     *         "name": "java3y",
     *         "label": "3y",
     *         "value": {
     *             "value": "{$tureName}",
     *             "color": "",
     *             "emphasis": ""
     *         }
     *     }, {
     *         "name": "java4y",
     *         "label": "3y",
     *         "value": {
     *             "value": "{$title}",
     *             "color": "",
     *             "emphasis": ""
     *         }
     *     }, {
     *         "name": "java5y",
     *         "label": "5y",
     *         "value": {
     *             "value": "关注我的公众号，更多干货",
     *             "color": "#ff0040",
     *             "emphasis": ""
     *         }
     *     }],
     *     "yyyImg": "",
     *     "yyyAge": "",
     *     "pagepath": ""
     * }
     */

    private String formId;

    private String link;

    // text是一个数组，此处我们定义为List
    private List<TextInfoBean> text;

    private String yyyImg;

    private String yyyAge;

    private String pagePath;


}
