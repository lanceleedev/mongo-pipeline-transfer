package com.mongodb.pipeline.transfer.parse.stage;

import org.bson.conversions.Bson;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.UnwindOptions;

/**
 * Unwind 聚合管道解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public final class UnwindParse {
    private UnwindParse() {
    }

    /**
     * Unwind Bson 生成
     *
     * @param json Unwind 需要解析内容
     * @return
     */
    public static Bson process(String json) {
        Bson unwind = null;
        if (json.contains("{")) {
            JSONObject jsonObject = JSONObject.parseObject(json);
            String path = jsonObject.getString("path");
            String includeArrayIndex = jsonObject.getString("includeArrayIndex");
            Boolean preserveNullAndEmptyArrays = jsonObject.getBoolean("preserveNullAndEmptyArrays");
            if (null == includeArrayIndex && null == preserveNullAndEmptyArrays) {
                unwind = Aggregates.unwind(path);
            } else if (null == includeArrayIndex) {
                unwind = Aggregates.unwind(path, new UnwindOptions().preserveNullAndEmptyArrays(preserveNullAndEmptyArrays));
            } else if (null == preserveNullAndEmptyArrays) {
                unwind = Aggregates.unwind(path, new UnwindOptions().includeArrayIndex(includeArrayIndex));
            } else {
                unwind = Aggregates.unwind(path, new UnwindOptions().includeArrayIndex(includeArrayIndex)
                        .preserveNullAndEmptyArrays(preserveNullAndEmptyArrays));
            }
        } else {
            unwind = Aggregates.unwind(json);
        }
        return unwind;
    }
}
