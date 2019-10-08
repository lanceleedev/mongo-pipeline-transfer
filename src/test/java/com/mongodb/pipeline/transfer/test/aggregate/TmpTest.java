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

        MongoCollection<Document> collection = database.getCollection("SY_FeeInfo");
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
//        bsonList.add(Aggregates.project(
//                new Document("收入", "$Income")
//                        .append("收入等级", new Document("$switch",
//                                new Document("branches", Arrays.asList(
//                                        new Document("case", new Document("$gte", Arrays.asList(new Document("$avg", "$Income"), 90))).append("then", "Doing great!"),
//                                        new Document("case", new Document("$and", Arrays.asList(
//                                                new Document("$gte", Arrays.asList(new Document("$avg", "$Income"), 80)),
//                                                new Document("$lt", Arrays.asList(new Document("$avg", "$Income"), 90))
//                                        ))).append("then", "Doing pretty well."),
//                                        new Document("case", new Document("$lt", Arrays.asList(new Document("$avg", "$Income"), 80))).append("then", "Needs improvement.")))
//                                        .append("default", "No scores found.")))
//        ));

//        {
//            $group: {
//                _id : {
//                    收入状态: "$IncomeStatus"
//                },
//                平均收入:{ $avg : "$Income" }
//
//            }
//        },{
//            $project: {
//                收入状态: "$_id.收入状态",
//                        平均收入: "$平均收入"
//            }
//        }
        bsonList.add(Aggregates.group(new Document("收入状态", "$IncomeStatus"), Accumulators.avg("平均收入", "$Income")));
        bsonList.add(Aggregates.project(new Document("收入状态" , "$_id.收入状态").append("平均收入","$平均收入")));

        return bsonList;
    }
}
