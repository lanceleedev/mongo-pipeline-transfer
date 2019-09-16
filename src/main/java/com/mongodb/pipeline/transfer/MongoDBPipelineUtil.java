/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.helper.StageHelper;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MongoDB 聚合管道 工具类
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class MongoDBPipelineUtil {

    /**
     * <p>将聚合JSON字符串转换为MongoDB Client需要的Bson格式</p>
     * Note:必须传入JSON数组
     *
     * @param json 聚合JSON字符串
     */
    public static List<Bson> aggregateJson2BsonList(String json) {
        JSONArray array = JSONObject.parseArray(json);
        List<Bson> bsonList = new ArrayList<>(array.size());

        Iterator<? extends Map.Entry<String, ?>> iterator;
        Map.Entry<String, ?> next;
        for (int i = 0, len = array.size(); i < len; i++) {
            JSONObject obj = (JSONObject) array.get(i);
            iterator = obj.entrySet().iterator();
            next = iterator.next();

            bsonList.add(StageHelper.parse(next.getKey(), next.getValue().toString()));
        }
        return bsonList;
    }
}
