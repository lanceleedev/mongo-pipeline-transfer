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
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;

/**
 * 表达式操作符解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class ExpOperation {
    /**
     * <p>cond 操作符解析</p>
     * @param json
     * @return
     */
    public static Document cond(String json) {
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        String IF = null;
        Object THEN = null;
        Object ELSE = null;
        while (iterator.hasNext()) {
            Map.Entry<String, ?> next = iterator.next();
            String key = next.getKey().toString().trim();
            Object val = next.getValue();
            if (key.equals("if")) {
                IF = val.toString().trim();
            } else if (key.equals("then")) {
                THEN = val;
            } else {
                ELSE = val;
            }
        }

        Iterator<? extends Map.Entry<String, ?>> ifIterator = JSONUtils.getJSONObjectIterator(IF);
        Map.Entry<String, ?> next = ifIterator.next();
        String operation = next.getKey().toString().trim();
        JSONArray params = JSONObject.parseArray(next.getValue().toString());

        Document cond = new Document();
        if (null != ELSE) {
            if (ELSE.toString().contains("$cond")) {
                Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(ELSE.toString().trim());
                Map.Entry<String, ?> tmpNext = tmpIter.next();
                cond = new Document("$cond", Arrays.asList(new Document(operation, Arrays.asList(params.getString(0), params.get(1))), THEN, cond(tmpNext
                        .getValue().toString().trim())));
            } else {
                cond = new Document("$cond", Arrays.asList(new Document(operation, Arrays.asList(params.getString(0), params.get(1))), THEN, ELSE));
            }
        }
        return cond;
    }

    /**
     * <p>ifNull操作符解析</p>
     * Sample:<br>
     * ["$money",""]
     * @param json
     * @return
     */
    public static Document ifNull(String json) {
        JSONArray array = JSONObject.parseArray(json);
        String field = null;
        Object value = null;
        for (Object obj : array) {
            if (obj.toString().startsWith("$")) {
                field = obj.toString().trim();
            } else {
                value = obj;
            }
        }
        return new Document("$ifNull", Arrays.asList(field, value));
    }

    /**
     * <p>multiply操作符转换</p>
     * Sample:<br>
     * ["$money","$number",{"NumberLong" : 1}]
     * @param json
     * @return
     */
    public static Document multiply(String json) {
        JSONArray array = JSONObject.parseArray(json);
        Object[] values = new Object[array.size()];
        for (int i = 0, len = array.size(); i < len; i++) {
            Object obj = array.get(i);
            if (obj.toString().startsWith("{")) {
                Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(obj.toString().trim());
                Map.Entry<String, ?> tmpNext = tmpIter.next();
                values[i] = TypesHelper.parse(tmpNext.getKey(), tmpNext.getValue());
            } else {
                values[i] = obj;
            }
        }
        return new Document("$multiply", Arrays.asList(values));
    }
}
