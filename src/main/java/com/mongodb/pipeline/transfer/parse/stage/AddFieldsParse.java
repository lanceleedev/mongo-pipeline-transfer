package com.mongodb.pipeline.transfer.parse.stage;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * AddFields stage解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class AddFieldsParse {
    private AddFieldsParse() {
    }

    /**
     * 生成 AddField Bson
     * @param json AddField 需要解析内容
     * @return
     */
    public static Bson process(String json) {
        List<Field<?>> fields = new ArrayList<>();

        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        Map.Entry<String, ?> next;
        String key;
        String value;
        while (iterator.hasNext()) {
            next = iterator.next();
            key = next.getKey().trim();
            value = next.getValue().toString().trim();
            if (-1 != value.indexOf(Constants.LBRACE)) {
                fields.add(new Field<>(key, ExpressionHelper.parse(value)));
            } else {
                fields.add(new Field<>(key, TypesHelper.parse(value)));
            }
        }
        return Aggregates.addFields(fields);
    }

}
