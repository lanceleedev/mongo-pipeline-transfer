package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import com.sun.org.apache.bcel.internal.generic.SWITCH;
import org.bson.Document;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * 条件表达式操作解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/18     Create this file
 * </pre>
 */
public final class ConditionalExpressionOperators extends Operators {
    private ConditionalExpressionOperators() {
    }

    /**
     * cond 操作符解析
     *
     * @param expression
     * @return
     */
    public static Document cond(String expression) {
        Iterator<? extends Map.Entry<String, ?>> iterator = JSONUtils.getJSONObjectIterator(expression);
        Object condIf = null;
        Object condThen = null;
        Object condElse = null;
        while (iterator.hasNext()) {
            Map.Entry<String, ?> next = iterator.next();
            String key = next.getKey().trim();
            String val = next.getValue().toString().trim();

            Object tmp = getExpressionValue(val);
            if ("if".equals(key)) {
                condIf = tmp;
            } else if ("then".equals(key)) {
                condThen = tmp;
            } else {
                condElse = tmp;
            }
        }

        return new Document(OperatorExpressionConstants.COND, Arrays.asList(condIf, condThen, condElse));
    }

    /**
     * ifNull 操作符解析
     * { $ifNull: [ <expression>, <replacement-expression-if-null> ] }
     *
     * @param expression
     * @return
     */
    public static Document ifNull(String expression) {
        JSONArray array = JSONObject.parseArray(expression);
        String str1 = array.get(0).toString().trim();
        String str2 = array.get(1).toString().trim();

        Object fieldExpression = null;
        if (str1.startsWith("$")) {
            fieldExpression = str1;
        } else {
            fieldExpression = TypesHelper.parse(str1);
        }

        Object replacement = null;
        if (str2.startsWith("$")) {
            replacement = str2;
        } else {
            replacement = TypesHelper.parse(str2);
        }
        return new Document(OperatorExpressionConstants.IF_NULL, Arrays.asList(fieldExpression, replacement));
    }

    /**
     * switch 操作符解析
     * <code>
     * $switch: {
     *    branches: [
     *       { case: <expression>, then: <expression> },
     *       { case: <expression>, then: <expression> },
     *       ...
     *    ],
     *    default: <expression>  // Optional.
     * }
     * </code>
     *
     * eg：
     * <code>
     * {
     *      $switch: {
     *      branches: [
     *        { case: { $eq: [ { $type: "$convertedPrice" }, "string" ] }, then: "NaN" },
     *        { case: { $eq: [ { $type: "$convertedQty" }, "string" ] }, then: "NaN" },
     *      ],
     *      default: { $multiply: [ "$convertedPrice", "$convertedQty" ] }
     *  }
     *  </code>
     *
     * @param json switch 操作符内容
     * @return
     */
    public static Document mSwitch(String json) {
        JSONObject obj = JSONObject.parseObject(json);
        String switchBranches = obj.getString(Constants.SWITCH_BRANCHES);
        Object switchDefault = obj.get(Constants.SWITCH_DEFAULT);

        JSONArray array = JSONObject.parseArray(switchBranches);
        Object[] branches = new Object[array.size()];
        for (int i = 0, len = array.size(); i < len; i++) {
            String caseJson = array.get(i).toString();
            JSONObject objTmp = JSONObject.parseObject(caseJson);
            branches[i] = new Document(Constants.SWITCH_CASE, getExpressionValue(objTmp.getString(Constants.SWITCH_CASE)))
                    .append(Constants.SWITCH_THEN, getExpressionValue(objTmp.getString(Constants.SWITCH_THEN)));
        }
        Document switchDocument = new Document(Constants.SWITCH_BRANCHES, Arrays.asList(branches));

        // optional default handle
        if (null != switchDefault) {
            switchDocument.append(Constants.SWITCH_DEFAULT, "No scores found.");
        }

        return new Document(OperatorExpressionConstants.SWITCH, switchDocument);
    }
}
