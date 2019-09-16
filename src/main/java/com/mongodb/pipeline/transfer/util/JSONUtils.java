package com.mongodb.pipeline.transfer.util;

import java.util.Iterator;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

/**
 * <p>JSON tools</p>
 * > 依赖于 FastJson
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public final class JSONUtils {
    private JSONUtils() {
    }

    /**
     * 解析Json，JSONObject对应的Set的迭代器
     *
     * @param json
     * @return
     */
    public static Iterator<? extends Map.Entry<String, ?>> getJSONObjectIterator(String json) {
        JSONObject obj = JSONObject.parseObject(json);
        return obj.entrySet().iterator();
    }

    /**
     * JSON预处理。<br/>
     * fastjson不支持NumberLong和new Date(),需要处理成字符串.
     * 最终格式："NumberLong('0')", "new Date()"
     */
    public static String fastjsonParsePreDeal(String json) {
        final String numberLong = "NumberLong";
        final String date = "new Date";

        return patternDeal(date, patternDeal(numberLong, json));
    }

    private static String patternDeal(String pattern, String json) {
        StringBuilder tmp = new StringBuilder(json.length() + 10);
        final int len = 30;
        int index;
        int start = 0;
        int end = 0;
        while (-1 != (index = json.indexOf(pattern))) {
            tmp.append(json.substring(0, index)).append("\"").append(pattern).append("(");

            start = index + pattern.length() + 1;
            /**
             * 括号内没有字符
             */
            if (')' == json.charAt(start)) {
                tmp.append(")\"");
                end = start;
            } else {
                tmp.append("\'");
                for (int i = start + 1; i < start + 1 + len; i++) {
                    if (')' == json.charAt(i)) {
                        end = i;
                        break;
                    }
                }
                // 截取引号内的内容
                tmp.append(json.substring(start + 1, end - 1)).append("\')\"");
            }

            json = json.substring(end + 1);
        }
        tmp.append(json);
        return tmp.toString();
    }

}

