package com.mongodb.pipeline.transfer.helper;

import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.TypesHelper;

/**
 * 操作helper
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/11/5     Create this file
 * </pre>
 */
public final class OperatorHelper {
    private OperatorHelper() {

    }

    /**
     * 获取表达式的值
     * @param expression 表达式
     * @return
     */
    public static Object getExpressionValue(String expression) {
        /**
         * 处理次序为表达式 > 数据类型 > 字符串
         */
        if (expression.contains(Constants.LBRACE)) {
            return ExpressionHelper.parse(expression);
        }
        if (TypesHelper.typeCheck(expression)) {
            return TypesHelper.parse(expression);
        }
        return expression;
    }
}
