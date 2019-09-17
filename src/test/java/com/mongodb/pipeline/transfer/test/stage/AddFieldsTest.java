package com.mongodb.pipeline.transfer.test.stage;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.pipeline.transfer.helper.StageHelper;
import com.mongodb.pipeline.transfer.test.util.JsonUtilsTest;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public class AddFieldsTest {
    @Test
    public void sample1Test() {
        String json = "{ChannelFee:   NumberLong(\"0\")  ,ChannelFeeStatus   :   NumberLong(\"10\")}";
        json = JSONUtils.fastjsonParsePreDeal(json);
        Bson addFields = StageHelper.parse("$addFields", json);
        System.out.println(addFields);

        List<Field<?>> fields = new ArrayList<>();
        fields.add(new Field<>("ChannelFee", new BsonInt32(0)));
        fields.add(new Field<>("ChannelFeeStatus", new BsonInt32(10)));
        Bson result = Aggregates.addFields(fields);
        System.out.println(result);
    }

    @Test
    public void sample2Test() {
        String json = "{Profit: {$subtract: [ {$subtract: [ \"$Income\", {$ifNull: [ \"$ChannelFee\", 0 ]} ]}, {$ifNull: [ \"$BankChannelFee\", 0 ]}]}," +
                "ProfitStatus: {$cond: {if: {$or: [{$eq: [\"$IncomeStatus\", 30]}, {$eq: [\"$IncomeStatus\", 40]}, {$eq: [\"$ChannelFeeStatus\", 30]}, {$eq: [\"$BankChannelFeeStatus\", 30]}]}, then: 30, else: {$cond: {if: {$and: [{$eq: [\"$IncomeStatus\", 10]}, {$eq: [\"$ChannelFeeStatus\", 10]}, {$eq: [\"$BankChannelFeeStatus\", 10]}]}, then: 10, else: 20}}}}}";
        json = JSONUtils.fastjsonParsePreDeal(json);
        Bson addFields = StageHelper.parse("$addFields", json);
        System.out.println(addFields);

        Bson result = Aggregates.addFields(
                new Field<>("Profit", new Document("$subtract", Arrays.asList(
                        new Document("$subtract", Arrays.asList(
                                "$Income", new Document("$ifNull", Arrays.asList("$ChannelFee", 0)))),
                        new Document("$ifNull", Arrays.asList("$BankChannelFee", 0)))
                )),
                new Field<>("ProfitStatus", new Document("$cond", Arrays.asList(
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
                        )))
                )
        );
        System.out.println(result);
    }

    @Test
    public void sample3Test() {
        String json = "{\n" +
                "\t\t ProfitInversion: {$cond: {if: {$lt: [\"$Profit\", 0]}, then: 1, else: 0}},\n" +
                "\t\t OperateTime: new Date()\n" +
                "\t}";
        json = JSONUtils.fastjsonParsePreDeal(json);
        Bson addFields = StageHelper.parse("$addFields", json);

        Bson result = Aggregates.addFields(
                new Field<>("ProfitInversion", new Document("$cond", Arrays.asList(
                        new Document("$lt", Arrays.asList("$Profit", 0)),
                        1,
                        0)
                )),
                new Field<>("OperateTime", new Date())
        );
        System.out.println(result);
    }
}
