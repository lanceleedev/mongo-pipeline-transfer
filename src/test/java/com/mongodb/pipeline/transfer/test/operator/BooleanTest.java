package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.parse.operator.ComparisonExpressionOperators;
import com.mongodb.pipeline.transfer.parse.operator.type.BooleanOperators;
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
public class BooleanTest {
    @Test
    public void sample1Test() {
        String json = "[{$eq: [\"$IncomeStatus\", 30]}, {$eq: [\"$IncomeStatus\", 40]}, {$eq: [\"$ChannelFeeStatus\", 30]}, {$eq: [\"$BankChannelFeeStatus\", 30]}]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document or = BooleanOperators.or(json);
        System.out.println(or);

        Document result = new Document("$or", Arrays.asList(
                new Document("$eq", Arrays.asList("$IncomeStatus", 30)),
                new Document("$eq", Arrays.asList("$IncomeStatus", 40)),
                new Document("$eq", Arrays.asList("$ChannelFeeStatus", 30)),
                new Document("$eq", Arrays.asList("$BankChannelFeeStatus", 30))
        ));
        System.out.println(result);
    }

    @Test
    public void sample2Test() {
        String json = "[{$eq: [\"$IncomeStatus\", 30]}, {$eq: [\"$IncomeStatus\", 40]}, {$eq: [\"$ChannelFeeStatus\", 30]}, {$eq: [\"$BankChannelFeeStatus\", 30]}]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document or = BooleanOperators.or(json);
        System.out.println(or);

        Document result = new Document("$or", Arrays.asList(
                new Document("$eq", Arrays.asList("$IncomeStatus", 30)),
                new Document("$eq", Arrays.asList("$IncomeStatus", 40)),
                new Document("$eq", Arrays.asList("$ChannelFeeStatus", 30)),
                new Document("$eq", Arrays.asList("$BankChannelFeeStatus", 30))
        ));
        System.out.println(result);
    }

    @Test
    public void sample3Test() {
        String json = "{ $and : [ { $gte : [ { $avg : \"$Income\" }, 80 ] },{ $lt : [ { $avg : \"$Income\" }, 90 ] } ] }";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document and = ExpressionHelper.parse(json);
        System.out.println(and);

        Document result = new Document("$and", Arrays.asList(
                new Document("$gte", Arrays.asList(new Document("$avg", "$Income"), 80)),
                new Document("$lt", Arrays.asList(new Document("$avg", "$Income"), 90))
        ));
        System.out.println(result);
    }




}
