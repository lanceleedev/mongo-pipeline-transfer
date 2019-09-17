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
public class CondTest {
    @Test
    public void sample1Test() {
        String json = "{if: {$and: [{$eq: [\"$IncomeStatus\", 10]}, {$eq: [\"$ChannelFeeStatus\", 10]}, {$eq: [\"$BankChannelFeeStatus\", 10]}]}, then: 10, else: 20}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document cond = ArithmeticExpressionOperators.cond(json);
        System.out.println(cond);

        Document result = new Document("$cond", Arrays.asList(
                new Document("$and", Arrays.asList(
                        new Document("$eq", Arrays.asList("$IncomeStatus", 10)),
                        new Document("$eq", Arrays.asList("$ChannelFeeStatus", 10)),
                        new Document("$eq", Arrays.asList("$BankChannelFeeStatus", 10))
                )),
                10,
                20)
        );
        System.out.println(result);
    }

    @Test
    public void sample2Test() {
        String json = "{if: {$lt: [\"$Profit\", 0]}, then: \"1\", else: \"0\"}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document cond = ArithmeticExpressionOperators.cond(json);
        System.out.println(cond);
    }

    @Test
    public void sample3Test() {
        String json = "{if: {$gte: [\"$Profit\", 0]}, then: NumberLong(\"11\"), else: NumberLong(\"21\")}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document cond = ArithmeticExpressionOperators.cond(json);
        System.out.println(cond);
    }

    @Test
    public void sample4Test() {
        String json = "{if: {$lt: [\"$Profit\", 0]}, then: new Date(), else: 0}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document cond = ArithmeticExpressionOperators.cond(json);
        System.out.println(cond);
    }

    @Test
    public void sample5Test() {
        String json = "{if: {$or: [{$eq: [\"$IncomeStatus\", 30]}, {$eq: [\"$IncomeStatus\", 40]}, {$eq: [\"$ChannelFeeStatus\", 30]}, {$eq: [\"$BankChannelFeeStatus\", 30]}]}, then: 30, else: {$cond: {if: {$and: [{$eq: [\"$IncomeStatus\", 10]}, {$eq: [\"$ChannelFeeStatus\", 10]}, {$eq: [\"$BankChannelFeeStatus\", 10]}]}, then: 10, else: 20}}}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        Document cond = ArithmeticExpressionOperators.cond(json);
        System.out.println(cond);

        Document result = new Document("$cond", Arrays.asList(
                new Document("$or", Arrays.asList(
                        new Document("$eq", Arrays.asList("$IncomeStatus", 30)),
                        new Document("$eq", Arrays.asList("$IncomeStatus", 40)),
                        new Document("$eq", Arrays.asList("$ChannelFeeStatus", 30)),
                        new Document("$eq", Arrays.asList("$BankChannelFeeStatus", 30))
                )),
                30,
                new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(
                                new Document("$eq", Arrays.asList("$IncomeStatus", 10)),
                                new Document("$eq", Arrays.asList("$ChannelFeeStatus", 10)),
                                new Document("$eq", Arrays.asList("$BankChannelFeeStatus", 10))
                        )),
                        10,
                        20)
                ))
        );
        System.out.println(result);
    }

}
