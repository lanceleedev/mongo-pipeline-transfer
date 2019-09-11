/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.parse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.pipeline.transfer.util.JSONUtils;

/**
 * match 聚合管道解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class MatchParse {
    /**
     * <p> Match Bson生成</p>
     * <h5>Note:规定key必须是String，value可以是字符串或者整型</h5>
     * eg：
     * $match: { $or: [ { score: { $gt: 70, $lt: 90 } }, { views: { $gte: 1000 } } ] }
     * $match: {author : "dave", "ResponseTime":{"$gte":"20171017152400","$lt":"20171017164640"}}
     * $match: {author : "dave", $or: [ { score: { $gt: 70, $lt: 90 } }, { views: { $gte: 1000 } } ]}
     * @param json
     * @return
     */
    public static Bson process(String json) {
        Bson match = null;
        List<Bson> filters = new ArrayList<Bson>();
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        Map.Entry<String, ?> next;
        String key = null;
        Object value;
        while (iterator.hasNext()) {
            next = iterator.next();
            key = next.getKey().trim();
            value = next.getValue();
            getFilters(key, value, filters);
        }

        if (1 == filters.size()) {
            match = Aggregates.match(filters.get(0));
        } else {
            match = Aggregates.match(Filters.and(filters));
        }
        return match;
    }

    /**
     * <p>迭代获取sub filter</p>
     * @param key key
     * @param value sub filter value
     * @param filters 存储sub filter的集合
     */
    private static void getFilters(String key, Object value, List<Bson> filters) {
        String valueStr = value.toString();
        if ("$or".equals(key)) {
            JSONArray array = JSONObject.parseArray(valueStr);
            List<Bson> orList = new ArrayList<Bson>();
            JSONObject obj;
            Iterator<? extends Map.Entry<String, ?>> iterator;
            Map.Entry<String, ?> next;
            for (int i = 0, len = array.size(); i < len; i++) {
                List<Bson> tmp = new ArrayList<Bson>();
                obj = (JSONObject) array.get(i);
                iterator = obj.entrySet().iterator();
                next = iterator.next();
                getFilters(next.getKey(), next.getValue(), tmp);
                orList.add(Filters.and(tmp));
            }
            filters.add(Filters.or(orList));
        } else {
            if (valueStr.contains("{")) {
                Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(valueStr);
                while (iterator.hasNext()) {
                    Map.Entry<String, ?> next = iterator.next();
                    String tmp1 = next.getKey().toString().trim();
                    Object tmp2 = next.getValue();
                    filters.add(getFilter(key, tmp2, tmp1));
                }
            } else {
                filters.add(Filters.eq(key, value));
            }
        }
    }

    /**
     * <p>Filter 部分解析</p>
     * @param key 字段
     * @param value 字段值
     * @param operation 操作
     * @return
     */
    private static Bson getFilter(String key, Object value, String operation) {
        Bson bson;
        switch (operation) {
        case "$eq":
            bson = Filters.eq(key, value);
            break;
        case "$gt":
            bson = Filters.gt(key, value);
            break;
        case "$gte":
            bson = Filters.gte(key, value);
            break;
        case "$lt":
            bson = Filters.lt(key, value);
            break;
        case "$lte":
            bson = Filters.lte(key, value);
            break;
        default:
            throw new RuntimeException("dont't support this operation!" + operation);
        }
        return bson;
    }
}
