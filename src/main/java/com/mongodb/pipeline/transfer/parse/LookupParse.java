/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Variable;
import com.mongodb.pipeline.transfer.util.JSONUtils;

/**
 * lookup 聚合管道解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class LookupParse {
    /**
     * <p>Lookup Match 生成</p>
     * @param json
     * @return
     */
    public static Bson process(String json) {
        Bson lookup = null;
        JSONObject jsonObject = JSONObject.parseObject(json);
        String from = jsonObject.getString("from");
        String as = jsonObject.getString("as");

        String localField = jsonObject.getString("localField");
        String foreignField = jsonObject.getString("foreignField");
        Object let = jsonObject.get("let");
        Object pipeline = jsonObject.get("pipeline");

        if (StringUtils.isNotBlank(localField) && StringUtils.isNotBlank(foreignField)) {
            lookup = Aggregates.lookup(from, localField, foreignField, as);
        } else if (null != pipeline) {
            List<Bson> pip = getPipeline(pipeline.toString().trim());
            if (null != let) {
                lookup = Aggregates.lookup(from, getVariables(let.toString().trim()), pip, as);
            } else {
                lookup = Aggregates.lookup(from, pip, as);
            }
        } else {
            throw new RuntimeException("lookup pipline config error! this lookup unused!");
        }

        return lookup;
    }

    /**
     * <p>解析Variables部分</p>
     * @param json
     * @return
     */
    private static List<Variable<String>> getVariables(String json) {
        List<Variable<String>> variables = new ArrayList<Variable<String>>();
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(json);
        while (iterator.hasNext()) {
            Map.Entry<String, ?> next = iterator.next();
            variables.add(new Variable<>(next.getKey(), next.getValue().toString()));
        }
        return variables;
    }

    /**
     * <p>解析pipline部分</p>
     * Note：仅支持$match
     * @param json
     * @return
     */
    private static List<Bson> getPipeline(String json) {
        List<Document> documentList = new ArrayList<Document>();
        JSONArray array = JSONObject.parseArray(json);
        JSONObject tmpObj;
        JSONArray tmpArr;
        Map.Entry<String, ?> next;
        for (int i = 0, len = array.size(); i < len; i++) {
            tmpObj = (JSONObject) array.get(i);
            tmpArr = tmpObj.getJSONObject("$match").getJSONObject("$expr").getJSONArray("$and");
            for (int j = 0, size = tmpArr.size(); j < size; j++) {
                JSONObject jsonObject = (JSONObject) tmpArr.get(j);
                next = jsonObject.entrySet().iterator().next();
                JSONArray jsonArray = JSONObject.parseArray(next.getValue().toString());
                documentList.add(new Document(next.getKey(), Arrays.asList(jsonArray.getString(0), jsonArray.getString(1))));
            }
        }
        return Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", documentList)))));
    }

}
