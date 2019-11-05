package com.mongodb.pipeline.transfer.helper;

import com.mongodb.client.model.Filters;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.parse.operator.AccumulatorsOperators;
import com.mongodb.pipeline.transfer.parse.operator.ComparisonExpressionOperators;
import com.mongodb.pipeline.transfer.parse.operator.ConditionalExpressionOperators;
import com.mongodb.pipeline.transfer.parse.operator.DateExpressionOperators;
import com.mongodb.pipeline.transfer.parse.operator.TypeExpressionOperators;
import com.mongodb.pipeline.transfer.parse.operator.type.BooleanOperators;
import com.mongodb.pipeline.transfer.parse.operator.type.StringOperators;
import com.mongodb.pipeline.transfer.parse.operator.ArithmeticExpressionOperators;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Iterator;
import java.util.Map;

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
     * @param value
     * @return
     */
    public static Document parse(String value) {
        Iterator<? extends Map.Entry<String, ?>> iter = JSONUtils.getJSONObjectIterator(value);
        Map.Entry<String, ?> next = iter.next();
        return parse(next.getKey().trim(), next.getValue().toString().trim());
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
            case OperatorExpressionConstants.SUBTRACT:
                operation = ArithmeticExpressionOperators.subtract(value);
                break;
            case OperatorExpressionConstants.MULTIPLY:
                operation = ArithmeticExpressionOperators.multiply(value);
                break;

            case OperatorExpressionConstants.AND:
                operation = BooleanOperators.and(value);
                break;
            case OperatorExpressionConstants.OR:
                operation = BooleanOperators.or(value);
                break;
            case OperatorExpressionConstants.NOT:
                operation = BooleanOperators.not(value);
                break;

            case OperatorExpressionConstants.CMP:
                operation = ComparisonExpressionOperators.cmp(value);
                break;
            case OperatorExpressionConstants.EQ:
                operation = ComparisonExpressionOperators.eq(value);
                break;
            case OperatorExpressionConstants.GT:
                operation = ComparisonExpressionOperators.gt(value);
                break;
            case OperatorExpressionConstants.GTE:
                operation = ComparisonExpressionOperators.gte(value);
                break;
            case OperatorExpressionConstants.LT:
                operation = ComparisonExpressionOperators.lt(value);
                break;
            case OperatorExpressionConstants.LTE:
                operation = ComparisonExpressionOperators.lte(value);
                break;
            case OperatorExpressionConstants.NE:
                operation = ComparisonExpressionOperators.ne(value);
                break;

            case OperatorExpressionConstants.COND:
                operation = ConditionalExpressionOperators.cond(value);
                break;
            case OperatorExpressionConstants.IF_NULL:
                operation = ConditionalExpressionOperators.ifNull(value);
                break;
            case OperatorExpressionConstants.SWITCH:
                operation = ConditionalExpressionOperators.mSwitch(value);
                break;

            case OperatorExpressionConstants.DATE_FROM_STRING:
                operation = DateExpressionOperators.dateFromString(value);
                break;
            case OperatorExpressionConstants.DATE_TO_STRING:
                operation = DateExpressionOperators.dateToString(value);
                break;
            case OperatorExpressionConstants.MONTH:
                operation = DateExpressionOperators.month(value);
                break;
            case OperatorExpressionConstants.YEAR:
                operation = DateExpressionOperators.year(value);
                break;

            case OperatorExpressionConstants.CONCAT:
                operation = StringOperators.concat(value);
                break;
            case OperatorExpressionConstants.SUBSTR:
                operation = StringOperators.substr(value);
                break;
            case OperatorExpressionConstants.TO_STRING:
                operation = StringOperators.toString(value);
                break;

            case OperatorExpressionConstants.CONVERT:
                operation = TypeExpressionOperators.convert(value);
                break;

            case OperatorExpressionConstants.AVG:
                operation = AccumulatorsOperators.avg(value);
                break;
            case OperatorExpressionConstants.MAX:
                operation = AccumulatorsOperators.max(value);
                break;
            case OperatorExpressionConstants.MIN:
                operation = AccumulatorsOperators.min(value);
                break;
            case OperatorExpressionConstants.SUM:
                operation = AccumulatorsOperators.sum(value);
                break;

            default:
                throw new RuntimeException("dont't support this operator!" + operate);
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
                throw new RuntimeException("dont't support this operator!" + operation);
        }
        return bson;
    }
}

