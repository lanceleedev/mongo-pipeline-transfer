package com.mongodb.pipeline.transfer.test.Operation;

import com.mongodb.pipeline.transfer.helper.FunctionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;

import java.util.Iterator;
import java.util.Map;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/11     Create this file
 * </pre>
 */
public class MathExpTest {
    public static void main(String[] args) {
        multiTest();
    }

    /**
     * add test
     */
    public static void addTest() {
        String test1 = "{$add:[\"$Amount\", \"$Fee\", \"$Benefit\"]}";
        String test2 = "{\"$add\":[\"$Amount\",{\"$numberLong\":\"1\"}]}";
        System.out.println(FunctionCall(test1));
        System.out.println(FunctionCall(test2));
    }

    public static void multiTest() {
        String json = "{\"$multiply\":[\"$FailTxAmount\",{\"$numberLong\":\"1\"}]}";
        System.out.println(FunctionCall(json));
    }

    private static Document FunctionCall(String json) {
        Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(json.trim());
        Map.Entry<String, ?> tmpNext = tmpIter.next();
        return FunctionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim());
    }
}
