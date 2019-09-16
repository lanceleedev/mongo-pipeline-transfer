package com.mongodb.pipeline.transfer.test.stage;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.pipeline.transfer.helper.StageHelper;
import com.mongodb.pipeline.transfer.test.util.JsonUtilsTest;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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

        List<Field<?>> fields = new ArrayList<>();
        fields.add(new Field<>("ChannelFee", new BsonInt32(0)));
        fields.add(new Field<>("ChannelFeeStatus", new BsonInt32(10)));
        Bson result = Aggregates.addFields(fields);
        System.out.println(result);
    }

    @Test
    public void sample2Test() {
        String json = "Profit: {$subtract: [ {$subtract: [ \"$Income\", {$ifNull: [ \"$ChannelFee\", 0 ]} ]}, {$ifNull: [ \"$BankChannelFee\", 0 ]}]}," +
                "ProfitStatus: {$cond: {" +
                "if: {$or: [{$eq: [\"$IncomeStatus\", 30]}, {$eq: [\"$IncomeStatus\", 40]}, {$eq: [\"$ChannelFeeStatus\", 30]}, {$eq: [\"$BankChannelFeeStatus\", 30]}]}," +
                " then: 30, " +
                "else: {$cond: {" +
                    "if: {$and: [{$eq: [\"$IncomeStatus\", 10]}, {$eq: [\"$ChannelFeeStatus\", 10]}, {$eq: [\"$BankChannelFeeStatus\", 10]}]}, " +
                    "then: 10, else: 20}}}}}";
        json = JSONUtils.fastjsonParsePreDeal(json);

        List<Field<?>> fields = new ArrayList<>();
        fields.add(new Field<>("Profit", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.AccountType", "11")), "个人", new Document("$cond",
                Arrays.asList(new Document("$eq", Arrays.asList("$_id.AccountType", "12")), "企业", ""))))));
        fields.add(new Field<>("ChannelFeeStatus", new BsonInt32(10)));
        Bson result = Aggregates.addFields(fields);
        System.out.println(result);

    }
}
