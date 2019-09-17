package com.mongodb.pipeline.transfer.parse.operator.type;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class BooleanOperators {
    private BooleanOperators() {
    }

    /**
     * { $and: [ <expression1>, <expression2>, ... ] }
     * <p>
     * Example	 	                                Result
     * { $and: [ 1, "green" ] }	 	 	 	 	 	true
     * { $and: [ ] }	 	 	 	 	 	 	 	true
     * { $and: [ [ null ], [ false ], [ 0 ] ] }	 	true
     * { $and: [ null, true ] }	 	 	 	 	 	false
     * { $and: [ 0, true ] }	 	 	 	 	 	false
     *
     * @param value
     * @return
     */
    public static Document and(String value) {
        return new Document(OperatorExpressionConstants.AND, Arrays.asList(getValues(value)));
    }

    /**
     * { $or: [ <expression1>, <expression2>, ... ] }
     *
     * @param value
     * @return
     */
    public static Document or(String value) {
        return new Document(OperatorExpressionConstants.OR, Arrays.asList(getValues(value)));
    }

    /**
     * { $not: [ <expression> ] }
     * TODO
     *
     * @param value
     * @return
     */
    public static Document not(String value) {
        return null;
    }

    /**
     * 获取值，特征：多个个表达式
     *
     * @param value
     * @return
     */
    private static Document[] getValues(String value) {
        JSONArray array = JSONObject.parseArray(value);
        Document[] values = new Document[array.size()];
        for (int i = 0, len = array.size(); i < len; i++) {
            values[i] = ExpressionHelper.parse(array.get(i).toString());
        }

        return values;
    }
}
