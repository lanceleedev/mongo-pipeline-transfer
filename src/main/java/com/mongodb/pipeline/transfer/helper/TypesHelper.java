package com.mongodb.pipeline.transfer.helper;

import com.mongodb.client.model.Field;
import com.mongodb.pipeline.transfer.constants.Constants;
import org.bson.BsonNumber;

import com.mongodb.pipeline.transfer.parse.operator.type.NumericOperators;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
    private static final String DATE = "Date";


    private TypesHelper() {
    }


    /**
     * 解析值类型.
     * 根据其中关键字判断
     *
     * @param value 值
     * @return
     */
    public static Object parse(String value) {

        if (-1 != value.indexOf(DATE)) {
            return new Date();
        }
        if (-1 != value.indexOf(Constants.NUMBER_LONG)) {
            return NumericOperators.numberLong(value);
        }
        return value;
    }

    /**
     * <p>数值解析</p>
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
                throw new RuntimeException("dont't support this operator!" + operation);
        }
        return operation;
    }
}
