# mongo-pipeline-transfer

# Features
提供将 `mongodb` 聚合管道 `Json` 转为 `mongo java` 客户端需要的 `Bson` 格式。

一般可以配合 `DataX` 插件 `mongodbpipelineReader` 使用。插件地址：

对应 `mongo-java-driver` 版本：3.10.2

# Quick Start
使用非常简单，仅仅需要增加一个 `pom` 依赖，然后通过 `MongoDBPipelineUtils.aggregateJson2BsonList(json)`转换即可。

#### Maven依赖
```

```
#### Java Code
```
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.pipeline.transfer.MongoDBPipelineUtils;

... 
String aggregate = "[{ $match : { Date:{$gte:\"20181112\", $lt:\"2018113\" }}},{ $lookup : { from: \"FeeInfo\", " +
                "localField: \"FeeVourcherID\", foreignField: \"FeeVourcherID\", as: \"fee\" }}...]";
List<Bson> bsonList = MongoDBPipelineUtils.aggregateJson2BsonList(aggregate);
MongoCursor<Document> dbCursor = col.aggregate(bsonList).allowDiskUse(true).iterator();
```

# 工作机制
将 `MongoDB pipeline` 字符串解析为 `List<Bson>` 进行聚合操作。
```
[{  
    $match : {
        Date:{$gte:"20181112", $lt:"2018113" }
    }
},{
    $lookup : {
        from: "FeeInfo",   
        localField: "FeeVourcherID",
        foreignField: "FeeVourcherID",
        as: "fee"
    }
},{
    $unwind :{ path:"$fee", preserveNullAndEmptyArrays: true}
},{
  $project: {
        SystemNo: "$SystemNo",
        Date: "$TxDate",
        Income: { $ifNull: [ "$fee.Income", "$Income" ] },
        IncomeStatus: {$cond: {if: {$gte: ["$fee.Income", NumberInt("0")]}, 
            then: "$fee.IncomeStatus", else: NumberInt("40")}}
    }
},{
    $addFields: {
        Status: NumberInt("0")
    }
}]
```
以上字符串将会被解析为
```
List<Bson> bsonList = new ArrayList<>();
bsonList.add(Aggregates.match(Filters.and(Filters.gte("Date", "20181112"),
        Filters.lt("Date", "2018113"))));

bsonList.add(Aggregates.lookup("FeeInfo", "FeeVourcherID", "FeeVourcherID", "fee"));

bsonList.add(Aggregates.unwind("$fee", new UnwindOptions().preserveNullAndEmptyArrays(true)));

bsonList.add(Aggregates.project(new Document("SystemNo", "$SystemNo")
        .append("Date", "$Date")
        .append("Income", new Document("$ifNull", Arrays.asList("$fee.Income", "$Income")))
        .append("IncomeStatus", new Document("$cond",
            Arrays.asList(new Document("$gte", Arrays.asList("$fee.Income", 0)), "$fee.IncomeStatus", 40)))
));

bsonList.add(Aggregates.addFields(new Field<>("Status", new BsonInt64(Long.parseLong("0")))));
```

# License
mongo-pipeline-transfer is released under the Apache 2.0 license.

# FAQ
如果你有任何问题，欢迎提Issus，或者通过邮件联系 `lanceleedev@outlook.com`。 