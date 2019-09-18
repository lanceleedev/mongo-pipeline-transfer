package com.mongodb.pipeline.transfer.parse.operator.type;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * 字符串操作符解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public final class StringOperators {
    private StringOperators() {
    }

    /**
     * substr操作符解析
     * { $substr: [ <string>, <start>, <length> ] }
     * Sample：
     * { $substr: [ { $ifNull: [ string, "XXXXXXXXX" ] }, start, length ] }
     *
     * @param json
     * @return
     */
    public static Document substr(String json) {
        Document substr = null;
        JSONArray params = JSONObject.parseArray(json);
        String str = params.getString(0);
        if (str.contains("{")) {
            Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(str.trim());
            Map.Entry<String, ?> tmpNext = tmpIter.next();
            substr = new Document("$substr", Arrays.asList(ExpressionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim()),
                    params.getIntValue(1), params.getIntValue(2)));
        } else {
            substr = new Document("$substr", Arrays.asList(str, params.getIntValue(1), params.getIntValue(2)));
        }
        return substr;
    }
}
