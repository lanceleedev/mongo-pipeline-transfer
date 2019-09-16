package com.mongodb.pipeline.transfer.parse.stage;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
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
     * <p>生成group Bson</p>
     *
     * @param json
     * @return
     */
    public static Bson process(String json) {
        Document _id = null;
        List<BsonField> fieldAccumulators = new ArrayList<BsonField>();

        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        Map.Entry<String, ?> next;
        String key;
        String value;
        while (iterator.hasNext()) {
            next = iterator.next();
            key = next.getKey().trim();
            value = next.getValue().toString().trim();
            if ("_id".equals(key)) {
                _id = getGroupByField(value);
            } else {
                fieldAccumulators.add(getAccumulator(key, value));
            }
        }

        return Aggregates.group(_id, fieldAccumulators);
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
     * @param field
     * @param value
     * @return
     */
    private static BsonField getAccumulator(String field, String value) {
        BsonField accumulator = null;
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(value);
        Map.Entry<String, ?> next = iterator.next();
        Object val = next.getValue();
        switch (next.getKey()) {
            case OperatorExpressionConstants.SUM:
                if (val.toString().contains("$cond")) {
                    Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(val.toString().trim());
                    Map.Entry<String, ?> tmpNext = tmpIter.next();
                    accumulator = Accumulators.sum(field, ExpressionHelper.parse("$cond", tmpNext.getValue().toString().trim()));
                } else {
                    accumulator = Accumulators.sum(field, val);
                }
                break;
            default:
                break;
        }
        return accumulator;
    }
}
