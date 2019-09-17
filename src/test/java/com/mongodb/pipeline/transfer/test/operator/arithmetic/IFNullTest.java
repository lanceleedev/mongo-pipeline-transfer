package com.mongodb.pipeline.transfer.test.operator.arithmetic;

import com.mongodb.pipeline.transfer.parse.operator.ArithmeticExpressionOperators;
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
public class IFNullTest {

    @Test
    public void sample1Test() {
        String json = "[\"$money\",\"\"]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document ifnull = ArithmeticExpressionOperators.ifNull(json);
        System.out.println(ifnull);
    }

    @Test
    public void sample2Test() {
        String json = "[ \"$ChannelFee\", NumberLong(\"0\") ]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document ifnull = ArithmeticExpressionOperators.ifNull(json);
        System.out.println(ifnull);

    }

    @Test
    public void sample3Test() {
        String json = "[ \"$fee.Income\", \"$Income\" ]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document ifnull = ArithmeticExpressionOperators.ifNull(json);
        System.out.println(ifnull);

        Document result = new Document("$ifNull", Arrays.asList("$fee.Income", "$Income"));
        System.out.println(result);

    }

}
