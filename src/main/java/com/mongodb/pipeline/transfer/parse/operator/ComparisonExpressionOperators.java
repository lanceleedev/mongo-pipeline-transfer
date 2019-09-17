package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
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
public final class ComparisonExpressionOperators {
    private ComparisonExpressionOperators() {
    }


    /**
     * { $cmp: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document cmp(String value) {
        return new Document(OperatorExpressionConstants.CMP, getValues(value));
    }

    /**
     * { $eq: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document eq(String value) {
        return new Document(OperatorExpressionConstants.EQ, getValues(value));
    }

    /**
     * { $gt: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document gt(String value) {
        return new Document(OperatorExpressionConstants.GT, getValues(value));
    }

    /**
     * { $gte: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document gte(String value) {
        return new Document(OperatorExpressionConstants.GTE, getValues(value));
    }

    /**
     * { $lt: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document lt(String value) {
        return new Document(OperatorExpressionConstants.LT, getValues(value));
    }

    /**
     * { $lte: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document lte(String value) {
        return new Document(OperatorExpressionConstants.LTE, getValues(value));
    }

    /**
     * { $ne: [ <expression1>, <expression2> ] }
     *
     * @param value
     * @return
     */
    public static Document ne(String value) {
        return new Document(OperatorExpressionConstants.NE, getValues(value));
    }

    /**
     * 获取值，特征：2个表达式
     *
     * @param value
     * @return
     */
    private static List<?> getValues(String value) {
        JSONArray array = JSONObject.parseArray(value);

        String expression1 = null;
        Object expression2 = null;

        for (int i = 0, len = array.size(); i < len; i++) {
            String obj = array.get(i).toString();
            if (obj.contains(Constants.DOLLAR)) {
                expression1 = obj;
            } else {
                expression2 = TypesHelper.parse(obj);
            }
        }

        return Arrays.asList(expression1, expression2);
    }
}
