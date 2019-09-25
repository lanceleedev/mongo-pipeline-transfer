package com.mongodb.pipeline.transfer.parse.operator.type;

import com.mongodb.pipeline.transfer.constants.Constants;
import org.bson.BsonDecimal128;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.types.Decimal128;

import java.math.BigDecimal;


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
     * numberInt操作符转换.<br>
     * numberInt('0')
     *
     * @param value
     * @return
     */
    public static BsonNumber numberInt(String value) {
        String tmp = value.split(Constants.APOSTROPHE)[1].trim();
        return new BsonInt32(Integer.parseInt(tmp));
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

    /**
     * numberDecimal 操作符转换.<br>
     * numberDecimal('0')
     *
     * @param value
     * @return
     */
    public static BsonNumber numberDecimal(String value) {
        String tmp = value.split(Constants.APOSTROPHE)[1].trim();
        return new BsonDecimal128(new Decimal128(new BigDecimal(tmp)));
    }

}
