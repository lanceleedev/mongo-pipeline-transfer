/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.operation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.bson.Document;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.helper.FunctionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;

/**
 * 字符串操作符解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class StringOperation {
    /**
     * <p>substr操作符解析</p>
     * 
     * { $substr: [ string, start, length ] }
     * { $substr: [ { $ifNull: [ string, "XXXXXXXXX" ] }, start, length ] }
     * @param json
     * @return
     */
    public static Document substr(String json) {
        Document substr = new Document();
        JSONArray params = JSONObject.parseArray(json);
        String str = params.getString(0);
        if (str.contains("{")) {
            Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(str.toString().trim());
            Map.Entry<String, ?> tmpNext = tmpIter.next();
            substr = new Document("$substr", Arrays.asList(FunctionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim()), params.getIntValue(1),
                    params.getIntValue(2)));
        } else {
            substr = new Document("$substr", Arrays.asList(str, params.getIntValue(1), params.getIntValue(2)));
        }
        return substr;
    }
}
