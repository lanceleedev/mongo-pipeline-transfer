package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.BsonField;
import com.mongodb.pipeline.transfer.helper.GroupStageAccumulatorHelper;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.parse.operator.AccumulatorsOperators;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.BsonInt64;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;

/**
 * 累加操作测试
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/18     Create this file
 * </pre>
 */
public class AccumulatorsTest {
    @Test
    public void sample1Test() {
        String json = "{$ifNull: [ \"$TxAmount\", NumberLong(\"0\") ]}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        BsonField sum = GroupStageAccumulatorHelper.parse("TotalTxAmount","$sum",json);
        System.out.println(sum);

        BsonField result = Accumulators.sum("TotalTxAmount",
                new Document("$ifNull", Arrays.asList("$TxAmount", new BsonInt64(Long.parseLong("0")))));
        System.out.println(result);
    }

    @Test
    public void sample2Test() {
        String json = "[ \"$final\", \"$midterm\" ]";
        json = JSONUtils.fastjsonParsePreDeal(json);

        BsonField sum = AccumulatorsOperators.sum("TotalTxAmount", json);
        System.out.println(sum);

        BsonField result = Accumulators.sum("TotalTxAmount", Arrays.asList("$final", "$midterm"));
        System.out.println(result);

    }

    @Test
    public void sample3Test() {
        String json = "{$cond: {if: {$eq: [\"$status\", 20]}, then: \"$money\", else: 0}}";
        json = JSONUtils.fastjsonParsePreDeal(json);
        BsonField sum = AccumulatorsOperators.sum("TotalTxAmount", json);
        System.out.println(sum);
    }

    @Test
    public void avgTest() {
        String json = "{ $avg : \"$Income\" }";
        json = JSONUtils.fastjsonParsePreDeal(json);
        Document avg = ExpressionHelper.parse(json);
        System.out.println(avg);


    }


}
