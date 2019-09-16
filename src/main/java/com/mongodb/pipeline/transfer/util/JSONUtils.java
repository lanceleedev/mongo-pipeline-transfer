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
public final class JSONUtils {
    private JSONUtils() {
    }

    /**
     * 解析Json，JSONObject对应的Set的迭代器
     * @param json
     * @return
     */
    public static Iterator<? extends Map.Entry<String, ?>> getJSONObjectIterator(String json) {
        JSONObject obj = JSONObject.parseObject(json);
        return obj.entrySet().iterator();
    }
}

