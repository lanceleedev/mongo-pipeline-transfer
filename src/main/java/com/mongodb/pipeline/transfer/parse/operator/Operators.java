package com.mongodb.pipeline.transfer.parse.operator;

import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;

/**
 * 基类
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public abstract class Operators {

    /**
     * 获取表达式的值
     * @param expression 表达式
     * @return
     */
    protected static Object getExpressionValue(String expression) {
        if (expression.contains(Constants.LBRACE)) {
            return ExpressionHelper.parse(expression);
        }
        return expression;
    }
}
