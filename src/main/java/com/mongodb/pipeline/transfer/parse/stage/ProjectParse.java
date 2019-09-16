package com.mongodb.pipeline.transfer.parse.stage;

import com.mongodb.client.model.Aggregates;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Iterator;
import java.util.Map;

/**
 * Project 聚合管道解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public final class ProjectParse {

    private ProjectParse() {
    }

    /**
     * <p>生成 Project Bson</p>
     *
     * @param json
     * @return
     */
    public static Bson process(String json) {
        Document project = new Document();

        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        Map.Entry<String, ?> next;
        String field = null;
        Object value = null;
        while (iterator.hasNext()) {
            next = iterator.next();
            field = next.getKey().trim();
            value = next.getValue();
            if (value.toString().contains("{")) {
                Iterator<? extends Map.Entry<String, ?>> tmpIter = JSONUtils.getJSONObjectIterator(value.toString().trim());
                Map.Entry<String, ?> tmpNext = tmpIter.next();
                project.append(field, ExpressionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim()));
            } else {
                project.append(field, value);
            }
        }
        return Aggregates.project(project);
    }

}
