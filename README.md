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

List<Bson> bsonList = MongoDBPipelineUtils.aggregateJson2BsonList(aggregate);
MongoCursor<Document> dbCursor = col.aggregate(bsonList).allowDiskUse(true).iterator();
```

## 使用注意事项
- 聚合语句中数据格式为强校验，比如 `$subtract`，两个表达式数据格式应相同；
- 数据类型必须明确，不可用使用含义不明确的 `0`，数字应使用 `NumberLong("0")` 或者 字符应使用`"0"`；
```mongodb
{
    $addFields: {
        Num: NumberLong("0"),
        Profit: {$subtract: [ "$Income", {$ifNull: [ "$Fee", NumberLong("0") ]} ]}
    }
}
```

# TODO List
- 支持更多的 `pipeline stage` 和更多的操作符解析
- 传入 `JSON` 格式校验
- 