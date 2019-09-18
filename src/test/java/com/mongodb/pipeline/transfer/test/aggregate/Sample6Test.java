package com.mongodb.pipeline.transfer.test.aggregate;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.pipeline.transfer.MongoDBPipelineUtils;
import com.mongodb.pipeline.transfer.test.util.FileUtils;
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
public class Sample6Test {
    @Test
    public void SampleTest() {
        MongoClient mongoClient = new MongoClient("172.31.1.41", 27017);
        MongoDatabase database = mongoClient.getDatabase("costbenefitanalysis");

        List<Bson> parseBson = getParseBson();
        System.out.println(parseBson);
        List<Bson> compareBson = getCompareBson();


        MongoCollection<Document> collection = database.getCollection("SY_TxDetail");
        MongoCursor<Document> dbCursor = collection.aggregate(parseBson).allowDiskUse(true).iterator();
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
        String aggregateJson = FileUtils.readFileToString(resourcePath + "/src/test/resource/PipelineTestFile/14-Sample6.js");
        return MongoDBPipelineUtils.aggregateJson2BsonList(aggregateJson);
    }

    private List<Bson> getCompareBson() {
        List<Bson> list = new ArrayList<>();
        list.add(Aggregates.match(Filters.and(Filters.gte("TxDate", "20181112"),
                Filters.lt("TxDate", "2018113"))));

        List<Variable<String>> variables1 = Arrays.asList(new Variable<>("g_PaymentType", "$PaymentType"), new Variable<>("g_TxSn", "$TxSn"),
                new Variable<>("g_TxType", "$TxType"));
        List<Bson> pipeline1 = Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", Arrays.asList(
                new Document("$eq", Arrays.asList("$PaymentType", "$$g_PaymentType")),
                new Document("$eq", Arrays.asList("$TxType", "$$g_TxType"))))))));
        list.add(Aggregates.lookup("SY_BusinessTypeConfig", variables1, pipeline1, "config"));

        list.add(Aggregates.unwind("$config", new UnwindOptions().preserveNullAndEmptyArrays(true)));




        return list;
    }
}
