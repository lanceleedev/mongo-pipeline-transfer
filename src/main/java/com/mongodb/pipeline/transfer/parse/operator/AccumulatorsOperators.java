package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.BsonField;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.OperatorHelper;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import org.bson.Document;

import java.util.Arrays;

/**
 * 累加器操作
 * group 阶段需要单独处理
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class AccumulatorsOperators {

    private AccumulatorsOperators() {
    }

    /**
     * group 阶段 avg 操作解析
     * Usage:
     * { $avg: <expression> }
     * { $avg: [ <expression1>, <expression2> ... ]  }
     *
     * @param field      字段
     * @param expression 表达式
     * @return
     */
    public static BsonField avg(String field, String expression) {
        /**
         * 判断表达式个数
         */
        try {
            JSONArray array = JSONObject.parseArray(expression);
            return Accumulators.avg(field, Arrays.asList(getValues(array)));
        } catch (Exception e) {
            return Accumulators.avg(field, getValue(expression));
        }
    }

    /**
     * group 阶段 max 操作解析
     * Usage:
     * { $max: <expression> }
     * { $max: [ <expression1>, <expression2> ... ]  }
     *
     * @param field      字段
     * @param expression 表达式
     * @return
     */
    public static BsonField max(String field, String expression) {
        /**
         * 判断表达式个数
         */
        try {
            JSONArray array = JSONObject.parseArray(expression);
            return Accumulators.max(field, Arrays.asList(getValues(array)));
        } catch (Exception e) {
            return Accumulators.max(field, getValue(expression));
        }
    }

    /**
     * group 阶段 min 操作解析
     * Usage:
     * { $min: <expression> }
     * { $min: [ <expression1>, <expression2> ... ]  }
     *
     * @param field      字段
     * @param expression 表达式
     * @return
     */
    public static BsonField min(String field, String expression) {
        /**
         * 判断表达式个数
         */
        try {
            JSONArray array = JSONObject.parseArray(expression);
            return Accumulators.min(field, Arrays.asList(getValues(array)));
        } catch (Exception e) {
            return Accumulators.min(field, getValue(expression));
        }
    }

    /**
     * group 阶段 sum 操作解析
     * Usage:
     * { $sum: <expression> }
     * { $sum: [ <expression1>, <expression2> ... ]  }
     *
     * @param field      字段
     * @param expression 表达式
     * @return
     */
    public static BsonField sum(String field, String expression) {
        /**
         * 判断表达式个数
         */
        try {
            JSONArray array = JSONObject.parseArray(expression);
            return Accumulators.sum(field, Arrays.asList(getValues(array)));
        } catch (Exception e) {
            return Accumulators.sum(field, getValue(expression));
        }
    }

    /**
     * 获取值，特征：多个个表达式
     *
     * @param array 表达式转化的json数组
     * @return
     */
    private static Object[] getValues(JSONArray array) {
        Object[] expressions = new Object[array.size()];
        Object tmp = null;
        for (int i = 0, len = array.size(); i < len; i++) {
            expressions[i] = getValue(array.get(i));
        }
        return expressions;
    }

    /**
     *
     * @param tmp
     * @return
     */
    private static Object getValue(Object tmp) {
        if (tmp.toString().startsWith(Constants.LBRACE)) {
            return ExpressionHelper.parse(tmp.toString().trim());
        }
        return TypesHelper.parse(tmp.toString());
    }

    /**
     * $avg 操作符解析
     * { $avg: <expression> }
     * @param expression
     * @return
     */
    public static Document avg(String expression) {
        return new Document(OperatorExpressionConstants.AVG, OperatorHelper.getExpressionValue(expression));
    }

    public static Document max(String expression) {
        return new Document(OperatorExpressionConstants.MAX, OperatorHelper.getExpressionValue(expression));
    }

    public static Document min(String expression) {
        return new Document(OperatorExpressionConstants.MIN, OperatorHelper.getExpressionValue(expression));
    }

    public static Document sum(String expression) {
        return new Document(OperatorExpressionConstants.SUM, OperatorHelper.getExpressionValue(expression));
    }
}
