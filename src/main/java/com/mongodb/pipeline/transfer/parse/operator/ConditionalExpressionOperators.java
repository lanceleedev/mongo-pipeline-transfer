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
 * 条件表达式操作解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/18     Create this file
 * </pre>
 */
public final class ConditionalExpressionOperators {
    private ConditionalExpressionOperators() {
    }

    /**
     * <p>cond 操作符解析
     *
     * @param expression
     * @return
     */
    public static Document cond(String expression) {
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(expression);
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
            if ("if".equals(key)) {
                condIf = tmp;
            } else if ("then".equals(key)) {
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
     * @param expression
     * @return
     */
    public static Document ifNull(String expression) {
        JSONArray array = JSONObject.parseArray(expression);
        String str1 = array.get(0).toString().trim();
        String str2 = array.get(1).toString().trim();

        Object fieldExpression = null;
        if (str1.startsWith("$")) {
            fieldExpression = str1;
        } else {
            fieldExpression = TypesHelper.parse(str1);
        }

        Object replacement = null;
        if (str2.startsWith("$")) {
            replacement = str2;
        } else {
            replacement = TypesHelper.parse(str2);
        }
        return new Document(OperatorExpressionConstants.IF_NULL, Arrays.asList(fieldExpression, replacement));
    }

}
