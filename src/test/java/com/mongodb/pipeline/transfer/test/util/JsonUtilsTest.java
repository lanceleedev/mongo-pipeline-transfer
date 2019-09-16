package com.mongodb.pipeline.transfer.test.util;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.junit.Test;

/**
 * Json 工具类测试
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public class JsonUtilsTest {
    @Test
    public void NumberLongTest() {
        final String result = "{ChannelFee:\"NumberLong('0')\",ChannelFeeStatus:\"NumberLong('10')\"}";

        String json1 = "{ChannelFee:NumberLong('0'),ChannelFeeStatus:NumberLong('10')}";
        json1 = JSONUtils.fastjsonParsePreDeal(json1);
        JSONObject.parseObject(json1);
        assert result.equals(json1);

        String json2 = "{ChannelFee:NumberLong(\"0\"),ChannelFeeStatus:NumberLong(\"10\")}";
        json2 = JSONUtils.fastjsonParsePreDeal(json2);
        JSONObject.parseObject(json2);
        assert result.equals(json2);

    }

    @Test
    public void DateTest() {
        final String result = "{ProfitInversion: {$cond: {if: {$lt: [\"$Profit\", 0]}, then: 1, else: 0}},OperateTime: \"new Date()\"}";

        String json = "{ProfitInversion: {$cond: {if: {$lt: [\"$Profit\", 0]}, then: 1, else: 0}},OperateTime: new Date()}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        JSONObject.parseObject(json);
        assert result.equals(json);
    }

}
