package com.mongodb.pipeline.transfer.parse.operators;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * 表达式操作符解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日  Create this file
 * </pre>
 */
public final class ArithmeticExpressionOperators {
    private ArithmeticExpressionOperators() {
    }

    /**
     * add操作符转换
     * { $add: [ <expression1>, <expression2>, ... ] }
     *
     * @param json
     * @return
     */
    public static Document add(String json) {
        return new Document(OperatorExpressionConstants.ADD, Arrays.asList(getArrayExpression(json)));
    }

    /**
     * multiply操作符转换<br>
     * { $multiply: [ <expression1>, <expression2>, ... ] }
     *
     * @param json
     * @return
     */
    public static Document multiply(String json) {
        return new Document(OperatorExpressionConstants.MULTIPLY, Arrays.asList(getArrayExpression(json)));
    }

    /**
     * 数组类型表达式获取
     *
     * @param json
     * @return
     */
    public static Object[] getArrayExpression(String json) {
        JSONArray array = JSONObject.parseArray(json);
        Object[] values = new Object[array.size()];
        for (int i = 0, len = array.size(); i < len; i++) {
            Object obj = array.get(i);
            if (obj.toString().startsWith("{")) {
                Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(obj.toString().trim());
                Map.Entry<String, ?> tmpNext = tmpIter.next();
                values[i] = TypesHelper.numericParse(tmpNext.getKey(), tmpNext.getValue());
            } else {
                values[i] = obj;
            }
        }
        return values;
    }

    /**
     * <p>cond 操作符解析</p>
     *
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
            String key = next.getKey().trim();
            Object val = next.getValue();
            if ("if".equals(key)) {
                IF = val.toString().trim();
            } else if ("then".equals(key)) {
                THEN = val;
            } else {
                ELSE = val;
            }
        }

        Iterator<? extends Map.Entry<String, ?>> ifIterator = JSONUtils.getJSONObjectIterator(IF);
        Map.Entry<String, ?> next = ifIterator.next();
        String operation = next.getKey().trim();
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
     *
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
}
