package com.mongodb.pipeline.transfer.parse.operators.type;

import org.bson.BsonInt64;
import org.bson.BsonNumber;


/**
 * 数值类型操作符解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public final class NumericOperators {
    private NumericOperators() {
    }

    /**
     * <p>numberLong操作符转换</p>
     * <p>
     * { "$numberLong": "1" }
     *
     * @param value
     * @return
     */
    public static BsonNumber numberLong(Object value) {
        return new BsonInt64(Long.parseLong(value.toString()));
    }
}
