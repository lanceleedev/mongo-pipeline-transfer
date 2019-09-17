package com.mongodb.pipeline.transfer.test.aggregate;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.pipeline.transfer.MongoDBPipelineUtils;
import com.mongodb.pipeline.transfer.test.util.FileUtils;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.*;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public class Sample4Test {
    @Test
    public void SampleTest() {
        MongoClient mongoClient = new MongoClient("172.31.1.41", 27017);
        MongoDatabase database = mongoClient.getDatabase("costbenefitanalysis");

//        List<Bson> parseBson = getParseBson();
//        System.out.println(parseBson);
        List<Bson> compareBson = getCompareBson();


        MongoCollection<Document> collection = database.getCollection("SY_CollectedTxInfo");
        MongoCursor<Document> dbCursor = collection.aggregate(compareBson).allowDiskUse(true).iterator();
        int count = 0;
        while (dbCursor.hasNext()) {
            Document item = dbCursor.next();
            count++;
            System.out.println(item.toJson());
        }
        System.out.println(count);
        dbCursor.close();
        mongoClient.close();
    }

    private List<Bson> getParseBson() {
        String path = this.getClass().getResource("/").getPath();
        String resourcePath = path.substring(0, path.indexOf("target"));
        String aggregateJson = FileUtils.readFileToString(resourcePath + "/src/test/resource/PipelineTestFile/12-Sample4.js");
        return MongoDBPipelineUtils.aggregateJson2BsonList(aggregateJson);
    }

    private List<Bson> getCompareBson() {
        List<Bson> list = new ArrayList<>();
        list.add(Aggregates.match(Filters.and(Filters.gte("TxDate", "20181112"),
                Filters.lt("TxDate", "2018113"))));

        list.add(Aggregates.lookup("SY_FeeInfo", "FeeVourcherID", "FeeVourcherID", "fee"));

        list.add(Aggregates.unwind("$fee", new UnwindOptions().preserveNullAndEmptyArrays(true)));

        List<Variable<String>> variables1 = Arrays.asList(new Variable<>("g_InstitutionID", "$InstitutionID"), new Variable<>("g_TxSn", "$TxSn"),
                new Variable<>("g_TxType", "$TxType"));
        List<Bson> pipeline1 = Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", Arrays.asList(
                new Document("$eq", Arrays.asList("$InstitutionID", "$$g_InstitutionID")),
                new Document("$eq", Arrays.asList("$TxSn", "$$g_TxSn")),
                new Document("$eq", Arrays.asList("$TxType", "$$g_TxType"))))))));
        list.add(Aggregates.lookup("SY_ChannelCost", variables1, pipeline1, "channel"));

        list.add(Aggregates.unwind("$channel", new UnwindOptions().preserveNullAndEmptyArrays(true)));

        list.add(Aggregates.addFields(new Field<>("ChannelFee", new BsonInt64(Long.parseLong("0"))),
                new Field<>("ChannelFeeStatus", new BsonInt64(Long.parseLong("10")))));

        list.add(Aggregates.project(new Document("SystemNo", "$SystemNo")
                .append("FeeVourcherID", "$FeeVourcherID")
                .append("TxSn", "$TxSn")
                .append("TxDate", "$TxDate")
                .append("TxType", "$TxType")
                .append("ChannelID", "$ChannelID")
                .append("ChannelName", "$ChannelName")
                .append("InstitutionParentID", "$InstitutionParentID")
                .append("InstitutionParentName", "$InstitutionParentName")
                .append("InstitutionID", "$InstitutionID")
                .append("InstitutionName", "$InstitutionName")
                .append("CardType", "$channel.CardType")
                .append("PaymentType", "$channel.PaymentType")
                .append("TxAmount", "$TxAmount")
                .append("SplitType", "$SplitType")
                .append("Income", new Document("$ifNull", Arrays.asList("$fee.Income", "$Income")))
                .append("Deduction", "$Deduction")
                .append("IncomeStatus", new Document("$cond", Arrays.asList(new Document("$gte", Arrays.asList("$fee.Income", 0)), "$fee.IncomeStatus", 40)))
                .append("ChannelFee", "$ChannelFee")
                .append("ChannelFeeStatus", "$ChannelFeeStatus")
                .append("BankChannelFee", "$BankChannelFee")
                .append("BankChannelFeeStatus", "$BankChannelFeeStatus")
        ));

        list.add(Aggregates.addFields(
                new Field<>("Profit", new Document("$subtract", Arrays.asList(
                        new Document("$subtract", Arrays.asList(
                                "$Income", new Document("$ifNull", Arrays.asList("$ChannelFee", new BsonInt64(Long.parseLong("0")))))),
                        new Document("$ifNull", Arrays.asList("$BankChannelFee", new BsonInt64(Long.parseLong("0")))))
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
                )
        );

        list.add(Aggregates.addFields(
                new Field<>("ProfitInversion", new Document("$cond", Arrays.asList(
                        new Document("$lt", Arrays.asList("$Profit", 0)),
                        1,
                        0)
                )),
                new Field<>("OperateTime", new Date())
        ));

        return list;
    }
}
