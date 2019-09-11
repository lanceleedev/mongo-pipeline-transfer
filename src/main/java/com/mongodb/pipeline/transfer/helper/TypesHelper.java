/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.helper;

import org.bson.BsonNumber;

import com.mongodb.pipeline.transfer.operation.NumericOperation;

/**
 * 值类型操作符转换
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class TypesHelper {
    /**
     * 
     * <p>Description TODO</p>
     * @param operate
     * @param value
     * @return
     */
    public static BsonNumber parse(String operate, Object value) {
        BsonNumber operation = null;
        switch (operate) {
        case "$numberLong":
            operation = NumericOperation.numberLong(value);
            break;
        default:
            throw new RuntimeException("dont't support this operation!" + operation);
        }
        return operation;
    }
}
