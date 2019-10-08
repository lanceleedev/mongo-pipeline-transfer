package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import com.mongodb.pipeline.transfer.parse.operator.type.NumericOperators;
import org.bson.Document;

import java.util.*;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/17     Create this file
 * </pre>
 */
public final class ComparisonExpressionOperators extends Operators {
    private ComparisonExpressionOperators() {
    }


    /**
     * { $cmp: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document cmp(String value) {
        return new Document(OperatorExpressionConstants.CMP, Arrays.asList(getValue(value)));
    }

    /**
     * { $eq: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document eq(String value) {
        return new Document(OperatorExpressionConstants.EQ, Arrays.asList(getValue(value)));
    }

    /**
     * { $gt: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document gt(String value) {
        return new Document(OperatorExpressionConstants.GT, Arrays.asList(getValue(value)));
    }

    /**
     * { $gte: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document gte(String value) {
        return new Document(OperatorExpressionConstants.GTE, Arrays.asList(getValue(value)));
    }

    /**
     * { $lt: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document lt(String value) {
        return new Document(OperatorExpressionConstants.LT, Arrays.asList(getValue(value)));
    }

    /**
     * { $lte: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document lte(String value) {
        return new Document(OperatorExpressionConstants.LTE, Arrays.asList(getValue(value)));
    }

    /**
     * { $ne: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document ne(String value) {
        return new Document(OperatorExpressionConstants.NE, Arrays.asList(getValue(value)));
    }

    /**
     * 获取值，特征：2个表达式
     *
     * @param value
     * @return
     */
    private static Object[] getValue(String value) {
        JSONArray array = JSONObject.parseArray(value);

        Object[] exp = new Object[2];
        for (int i = 0, len = array.size(); i < len; i++) {
            exp[i] = getExpressionValue(array.getString(i));
        }

        return exp;
    }
}
