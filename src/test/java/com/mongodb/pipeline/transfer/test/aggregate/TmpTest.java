package com.mongodb.pipeline.transfer.test.aggregate;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UnwindOptions;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
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
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public class TmpTest {

    @Test
    public void SampleTest() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://cpcn:cpcn1234@172.31.1.226:27017,172.31.1.248:27017,172.31.1.228:27017,172.31.1.252:27017/?authSource=admin"));
        MongoDatabase database = mongoClient.getDatabase("costbenefitanalysis");

        List<Bson> compareBson = getCompareBson();

        MongoCollection<Document> collection = database.getCollection("SY_TxDetail");
        MongoCursor<Document> dbCursor = collection.aggregate(compareBson).allowDiskUse(true).iterator();
        while (dbCursor.hasNext()) {
            Document item = dbCursor.next();
            System.out.println(item.toJson());
        }
        dbCursor.close();
        mongoClient.close();
    }

    private List<Bson> getCompareBson() {
        List<Bson> bsonList = new ArrayList<>();
        bsonList.add(Aggregates.addFields(new Field<>("dateTime", new Document("$dateFromString", new Document("dateString",  "$交易日期").append("format","%Y%m%d")))
        ));


//        bsonList.add(Aggregates.group(new Document("收入状态", "$IncomeStatus"), Accumulators.avg("平均收入", "$Income")));
        bsonList.add(Aggregates.project(new Document("dateTime" , "$dateTime").append("交易日期","$交易日期")));


        return bsonList;
    }

    private List<Bson> getParseBson() {
        String json = "{$cond: {if: {$eq: [\"$MaxIncomeStatus\", NumberInt(\"10\")]}, then: NumberInt(\"10\"), else: {$cond: {if: {$gte: [\"$MaxIncomeStatus\", NumberInt(\"30\")]}, then: NumberInt(\"30\"), else: NumberInt(\"20\")}}}}";

        Bson bson = ExpressionHelper.parse(json);
        return null;
    }
}
