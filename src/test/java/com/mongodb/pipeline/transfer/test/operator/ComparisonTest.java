package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.pipeline.transfer.parse.operator.ArithmeticExpressionOperators;
import com.mongodb.pipeline.transfer.parse.operator.ComparisonExpressionOperators;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/17     Create this file
 * </pre>
 */
public class ComparisonTest {
    @Test
    public void sample1Test() {
        String json = "[\"$IncomeStatus\", 30]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document eq = ComparisonExpressionOperators.eq(json);
        System.out.println(eq);

        Document result = new Document("$eq", Arrays.asList("$IncomeStatus", 30));
        System.out.println(result);
    }

    @Test
    public void sample2Test() {
        String json = "[\"$IncomeStatus\", NumberLong(\"10\")]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document lt = ComparisonExpressionOperators.lt(json);
        System.out.println(lt);

        Document result = new Document("$lt", Arrays.asList("$IncomeStatus", 10));
        System.out.println(result);
    }
}
