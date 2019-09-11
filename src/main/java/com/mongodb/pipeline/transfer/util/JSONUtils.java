/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.util;

import java.util.Iterator;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

/**
 * <p>JSON tools</p>
 * > 依赖于 FastJson
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class JSONUtils {
    public static Iterator<? extends Map.Entry<String, ?>> getJSONObjectIterator(String json) {
        JSONObject obj = JSONObject.parseObject(json);
        return obj.entrySet().iterator();
    }
}

