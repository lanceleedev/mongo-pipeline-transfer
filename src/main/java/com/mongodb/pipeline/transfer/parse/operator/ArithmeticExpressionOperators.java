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
 * 算术表达式操作符解析
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
     * @param expression 表达式
     * @return
     */
    public static Document add(String expression) {
        return new Document(OperatorExpressionConstants.ADD, Arrays.asList(getArrayExpression(expression)));
    }

    /**
     * subtract操作符转换
     * { $subtract: [ <expression1>, <expression2> ] }
     *
     * @param expression 表达式
     * @return
     */
    public static Document subtract(String expression) {
        JSONArray array = JSONObject.parseArray(expression);
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
     * @param expression 表达式
     * @return
     */
    public static Document multiply(String expression) {
        return new Document(OperatorExpressionConstants.MULTIPLY, Arrays.asList(getArrayExpression(expression)));
    }

    /**
     * 数组类型表达式获取
     *
     * @param arrayExpression 数组表达式
     * @return
     */
    public static Object[] getArrayExpression(String arrayExpression) {
        JSONArray array = JSONObject.parseArray(arrayExpression);
        Object[] expressions = new Object[array.size()];
        for (int i = 0, len = array.size(); i < len; i++) {
            Object obj = array.get(i);
            if (obj.toString().startsWith(Constants.LBRACE)) {
                Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(obj.toString().trim());
                Map.Entry<String, ?> tmpNext = tmpIter.next();
                expressions[i] = TypesHelper.numericParse(tmpNext.getKey(), tmpNext.getValue());
            } else {
                expressions[i] = obj;
            }
        }
        return expressions;
    }
}
