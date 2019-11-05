package com.mongodb.pipeline.transfer.parse.operator.type;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.OperatorHelper;
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
     * @param expressions 表达式
     * @return
     */
    public static Document substr(String expressions) {
        Document substr = null;
        JSONArray params = JSONObject.parseArray(expressions);
        String str = params.getString(0);
        if (str.contains(Constants.LBRACE)) {
            Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(str.trim());
            Map.Entry<String, ?> tmpNext = tmpIter.next();
            substr = new Document("$substr", Arrays.asList(ExpressionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim()),
                    params.getIntValue(1), params.getIntValue(2)));
        } else {
            substr = new Document("$substr", Arrays.asList(str, params.getIntValue(1), params.getIntValue(2)));
        }
        return substr;
    }

    /**
     * concat 操作符解析
     * { $concat: [ <expression1>, <expression2>, ... ] }
     * eg:
     * { $concat: [ { $convert: { input: "$Income", to: "string"}}, " - ", "元" ] }
     *
     * @param expressions 表达式
     * @return
     */
    public static Document concat(String expressions) {
        JSONArray jsonArray = JSONObject.parseArray(expressions);

        Object[] expressionArr = new Object[jsonArray.size()];
        for (int i = 0, len = jsonArray.size(); i < len; i++) {
            expressionArr[i] = OperatorHelper.getExpressionValue(jsonArray.get(i).toString());
        }

        return new Document(OperatorExpressionConstants.CONCAT, Arrays.asList(expressionArr));
    }

    /**
     * toString 操作符解析
     * {
     * $toString: <expression>
     * }
     * 等价于
     * { $convert: { input: <expression>, to: "string" } }
     *
     * @param expression 表达式
     * @return
     */
    public static Document toString(String expression) {
        return new Document(OperatorExpressionConstants.TO_STRING, OperatorHelper.getExpressionValue(expression));
    }

}

