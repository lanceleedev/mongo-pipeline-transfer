package com.mongodb.pipeline.transfer.helper;

import com.mongodb.client.model.BsonField;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.parse.operator.AccumulatorsOperators;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/18     Create this file
 * </pre>
 */
public final class GroupStageAccumulatorHelper {
    private GroupStageAccumulatorHelper() {
    }

    /**
     * 累加器解析
     *
     * @param operate 操作符
     * @param expression 表达式
     * @return
     */
    public static BsonField parse(String field, String operate, String expression) {
        BsonField accumulator = null;
        switch (operate) {
            case OperatorExpressionConstants.AVG:
                accumulator = AccumulatorsOperators.avg(field, expression);
                break;
            case OperatorExpressionConstants.MAX:
                accumulator = AccumulatorsOperators.max(field, expression);
                break;
            case OperatorExpressionConstants.MIN:
                accumulator = AccumulatorsOperators.min(field, expression);
                break;
            case OperatorExpressionConstants.SUM:
                accumulator = AccumulatorsOperators.sum(field, expression);
                break;

            default:
                throw new RuntimeException("dont't support this accumulator!" + operate);
        }
        return accumulator;
    }
}
