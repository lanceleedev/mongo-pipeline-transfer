/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.parse;

import java.util.Iterator;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;
import com.mongodb.pipeline.transfer.helper.FunctionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;

/**
 * Project 聚合管道解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class ProjectParse {
    /**
     * <p>生成 Project Bson</p>
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
                project.append(field, FunctionHelper.parse(tmpNext.getKey(), tmpNext.getValue().toString().trim()));
            } else {
                project.append(field, value);
            }
        }
        return Aggregates.project(project);
    }

}
