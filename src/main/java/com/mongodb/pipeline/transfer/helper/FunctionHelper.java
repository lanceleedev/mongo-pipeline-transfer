/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.helper;

import com.mongodb.pipeline.transfer.constants.OperatorExpressionsConstants;
import com.mongodb.pipeline.transfer.parse.operation.StringOperation;
import com.mongodb.pipeline.transfer.parse.operation.ExpOperation;
import org.bson.Document;

/**
 * 函数处理
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年7月2日           Create this file
 * </pre>
 */
public final class FunctionHelper {
    private FunctionHelper() {
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
            case OperatorExpressionsConstants.ADD:
                operation = ExpOperation.add(value);
                break;
            case "$multiply":
                operation = ExpOperation.multiply(value);
                break;
            case "$substr":
                operation = StringOperation.substr(value);
                break;
            case "$cond":
                operation = ExpOperation.cond(value);
                break;
            case "$ifNull":
                operation = ExpOperation.ifNull(value);
                break;
            default:
                throw new RuntimeException("dont't support this operation!" + operation);
        }
        return operation;
    }
}

