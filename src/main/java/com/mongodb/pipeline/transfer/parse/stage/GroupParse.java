package com.mongodb.pipeline.transfer.parse.stage;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.pipeline.transfer.helper.GroupStageAccumulatorHelper;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * group 聚合管道解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public final class GroupParse {
    private GroupParse() {
    }

    /**
     * 生成Group Bson
     *
     * @param json Group 需要解析内容
     * @return
     */
    public static Bson process(String json) {
        Document groupBy = null;
        List<BsonField> fieldAccumulators = new ArrayList<>();

        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        Map.Entry<String, ?> next;
        String key;
        String value;
        while (iterator.hasNext()) {
            next = iterator.next();
            key = next.getKey().trim();
            value = next.getValue().toString().trim();
            if ("_id".equals(key)) {
                groupBy = getGroupByField(value);
            } else {
                fieldAccumulators.add(getAccumulator(key, value));
            }
        }

        return Aggregates.group(groupBy, fieldAccumulators);
    }

    /**
     * <p>group _id部分解析</p>
     *
     * @param json
     * @return
     */
    private static Document getGroupByField(String json) {
        Document fields = new Document();

        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        while (iterator.hasNext()) {
            Map.Entry<String, ?> next = iterator.next();
            String field = next.getKey().trim();
            Object value = next.getValue();
            String valStr = value.toString().trim();
            if (valStr.contains("{")) {
                Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(valStr);
                Map.Entry<String, ?> tmpNext = tmpIter.next();
                fields.append(field, ExpressionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim()));
            } else {
                fields.append(field, value);
            }
        }
        return fields;
    }

    /**
     * <p>group 表达式部分解析</p>
     * Note：操作符仅支持：$sum
     * eg：
     * money: {$sum: {$cond: {if: {$eq: ["$status", 20]}, then: "$money", else: 0}}}
     * money: {$sum: "$money"}
     *
     * @param field 字段
     * @param value
     * @return
     */
    private static BsonField getAccumulator(String field, String value) {
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(value);
        Map.Entry<String, ?> next = iterator.next();
        return GroupStageAccumulatorHelper.parse(field, next.getKey().trim(), next.getValue().toString().trim());
    }
}
