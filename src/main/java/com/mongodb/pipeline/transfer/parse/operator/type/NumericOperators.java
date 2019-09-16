package com.mongodb.pipeline.transfer.parse.operator.type;

import com.mongodb.pipeline.transfer.constants.Constants;
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
     * numberLong操作符转换<br>
     * { "$numberLong": "1" }
     *
     * @param value
     * @return
     */
    public static BsonNumber numberLong(Object value) {
        return new BsonInt64(Long.parseLong(value.toString()));
    }

    /**
     * numberLong操作符转换.<br>
     * NumberLong('0')
     *
     * @param value
     * @return
     */
    public static BsonNumber numberLong(String value) {
        String tmp = value.split(Constants.APOSTROPHE)[1].trim();
        return new BsonInt64(Long.parseLong(tmp));
    }
}
