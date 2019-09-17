package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
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
     * subtract操作符转换
     * { $subtract: [ <expression1>, <expression2> ] }
     *
     * @param json
     * @return
     */
    public static Document subtract(String json) {
        JSONArray array = JSONObject.parseArray(json);
        Object[] values = new Object[array.size()];
        for (int i = 0, len = array.size(); i < len; i++) {
            Object obj = array.get(i);
            if (obj.toString().startsWith(Constants.LBRACE)) {
                values[i] = ExpressionHelper.parse(obj.toString().trim());
            } else {
                values[i] = obj;
            }
        }

        return new Document(OperatorExpressionConstants.SUBTRACT, Arrays.asList(values));
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
        Object condIf = null;
        Object condThen = null;
        Object condElse = null;
        while (iterator.hasNext()) {
            Map.Entry<String, ?> next = iterator.next();
            String key = next.getKey().trim();
            String val = next.getValue().toString().trim();

            Object tmp = null;
            if (val.startsWith(Constants.LBRACE)) {
                tmp = ExpressionHelper.parse(val);
            } else {
                tmp = TypesHelper.parse(val);
            }
            if ("if" .equals(key)) {
                condIf = tmp;
            } else if ("then" .equals(key)) {
                condThen = tmp;
            } else {
                condElse = tmp;
            }
        }

        return new Document(OperatorExpressionConstants.COND, Arrays.asList(condIf, condThen, condElse));
    }

    /**
     * ifNull操作符解析
     * { $ifNull: [ <expression>, <replacement-expression-if-null> ] }
     *
     * @param json
     * @return
     */
    public static Document ifNull(String json) {
        JSONArray array = JSONObject.parseArray(json);
        String str1 = array.get(0).toString().trim();
        String str2 = array.get(1).toString().trim();

        Object expression = null;
        if (str1.startsWith("$")) {
            expression = str1;
        }else{
            expression = TypesHelper.parse(str1);
        }

        Object replacement = null;
        if (str2.startsWith("$")) {
            replacement = str2;
        }else{
            replacement = TypesHelper.parse(str2);
        }
        return new Document("$ifNull", Arrays.asList(expression, replacement));
    }
}
