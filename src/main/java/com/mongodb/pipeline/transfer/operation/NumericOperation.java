/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.operation;

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
public class NumericOperation {

    /**
     * <p>numberLong操作符转换</p>
     * 
     * { "$numberLong": "1" }
     * @param json
     * @return
     */
    public static BsonNumber numberLong(Object value) {
        return new BsonInt64(Long.parseLong(value.toString()));
    }
}