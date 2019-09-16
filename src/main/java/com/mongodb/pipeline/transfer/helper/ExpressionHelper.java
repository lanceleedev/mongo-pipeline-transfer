package com.mongodb.pipeline.transfer.helper;

import com.mongodb.client.model.Filters;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.parse.operators.type.StringOperators;
import com.mongodb.pipeline.transfer.parse.operators.ArithmeticExpressionOperators;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * 函数处理
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年7月2日           Create this file
 * </pre>
 */
public final class ExpressionHelper {
    private ExpressionHelper() {
    }

    /**
     * <p>解析函数，根据函数类型，选择对应的解析方式</p>
     *
     * @param operate 函数
     * @param value   需要解析内容
     * @return
     */
    public static Document parse(String operate, String value) {
        Document operation = null;
        switch (operate) {
            case OperatorExpressionConstants.ADD:
                operation = ArithmeticExpressionOperators.add(value);
                break;
            case "$multiply":
                operation = ArithmeticExpressionOperators.multiply(value);
                break;
            case "$substr":
                operation = StringOperators.substr(value);
                break;
            case "$cond":
                operation = ArithmeticExpressionOperators.cond(value);
                break;
            case "$ifNull":
                operation = ArithmeticExpressionOperators.ifNull(value);
                break;
            default:
                throw new RuntimeException("dont't support this operators!" + operation);
        }
        return operation;
    }

    /**
     * <p>比较表达式操作解析</p>
     *
     * @param key       字段
     * @param value     字段值
     * @param operation 操作
     * @return
     */
    public static Bson getComparison(String operation, String key, Object value) {
        Bson bson;
        switch (operation) {
            case "$eq":
                bson = Filters.eq(key, value);
                break;
            case "$gt":
                bson = Filters.gt(key, value);
                break;
            case "$gte":
                bson = Filters.gte(key, value);
                break;
            case "$lt":
                bson = Filters.lt(key, value);
                break;
            case "$lte":
                bson = Filters.lte(key, value);
                break;
            default:
                throw new RuntimeException("dont't support this operators!" + operation);
        }
        return bson;
    }
}

