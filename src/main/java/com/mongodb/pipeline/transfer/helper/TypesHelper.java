package com.mongodb.pipeline.transfer.helper;

import org.bson.BsonNumber;

import com.mongodb.pipeline.transfer.parse.operators.type.NumericOperators;

/**
 * 数据类型操作符转换.
 * 包括：基本数据类型、字符串、时间、对象、数组
 *
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日  Create this file
 * </pre>
 */
public final class TypesHelper {
    private TypesHelper() {
    }

    /**
     * <p>Description TODO</p>
     *
     * @param operate
     * @param value
     * @return
     */
    public static BsonNumber numericParse(String operate, Object value) {
        BsonNumber operation = null;
        switch (operate) {
            case "$numberLong":
                operation = NumericOperators.numberLong(value);
                break;
            default:
                throw new RuntimeException("dont't support this operators!" + operation);
        }
        return operation;
    }
}
