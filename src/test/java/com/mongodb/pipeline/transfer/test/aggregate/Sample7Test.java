package com.mongodb.pipeline.transfer.test.aggregate;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UnwindOptions;
import com.mongodb.client.model.Variable;
import com.mongodb.pipeline.transfer.MongoDBPipelineUtils;
import com.mongodb.pipeline.transfer.test.util.FileUtils;
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
public class Sample7Test {
    @Test
    public void SampleTest() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://cpcn:cpcn1234@172.31.1.226:27017,172.31.1.248:27017,172.31.1.228:27017,172.31.1.252:27017/?authSource=admin"));
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
        String aggregateJson = FileUtils.readFileToString(resourcePath + "/src/test/resource/PipelineTestFile/15-Sample7.js");
        return MongoDBPipelineUtils.aggregateJson2BsonList(aggregateJson);
    }

    private List<Bson> getCompareBson() {
        List<Bson> list = new ArrayList<>();

        return list;
    }
}
