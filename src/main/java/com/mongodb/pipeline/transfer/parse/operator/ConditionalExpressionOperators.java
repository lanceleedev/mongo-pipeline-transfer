package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.helper.OperatorHelper;
import com.mongodb.pipeline.transfer.helper.TypesHelper;
import org.bson.Document;

import java.util.Arrays;

/**
 * 条件表达式操作解析
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/18     Create this file
 * </pre>
 */
public final class ConditionalExpressionOperators {
    private ConditionalExpressionOperators() {
    }

    /**
     * cond 操作符解析
     * <code>
     * { $cond: { if: <boolean-expression>, then: <true-case>, else: <false-case-> } }
     * or
     * { $cond: [ <boolean-expression>, <true-case>, <false-case> ] }
     * </code>
     * @param json 操作符内容
     * @return
     */
    public static Document cond(String json) {
        // cond语法格式判断
        if (json.startsWith(Constants.LBRACKET)) {
            JSONArray array = JSONObject.parseArray(json);
            Object[] expressions = new Object[3];
            for (int i = 0, len = array.size(); i < len; i++) {
                expressions[i] = OperatorHelper.getExpressionValue(array.getString(i));
            }
            return new Document(OperatorExpressionConstants.COND, Arrays.asList(expressions));
        }

        JSONObject obj = JSONObject.parseObject(json);
        String condIf = obj.getString(Constants.COND_IF);
        String condThen = obj.getString(Constants.COND_THEN);
        String condElse = obj.getString(Constants.COND_ELSE);
        return new Document(OperatorExpressionConstants.COND, Arrays.asList(OperatorHelper.getExpressionValue(condIf),
                OperatorHelper.getExpressionValue(condThen), OperatorHelper.getExpressionValue(condElse)));
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
        String expStr = array.get(0).toString().trim();
        String repStr = array.get(1).toString().trim();

        Object fieldExpression = OperatorHelper.getExpressionValue(expStr);;
        Object replacement = OperatorHelper.getExpressionValue(repStr);

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
            branches[i] = new Document(Constants.SWITCH_CASE, OperatorHelper.getExpressionValue(objTmp.getString(Constants.SWITCH_CASE)))
                    .append(Constants.SWITCH_THEN, OperatorHelper.getExpressionValue(objTmp.getString(Constants.SWITCH_THEN)));
        }
        Document switchDocument = new Document(Constants.SWITCH_BRANCHES, Arrays.asList(branches));

        // optional default handle
        if (null != switchDefault) {
            switchDocument.append(Constants.SWITCH_DEFAULT, OperatorHelper.getExpressionValue(switchDefault.toString()));
        }

        return new Document(OperatorExpressionConstants.SWITCH, switchDocument);
    }
}
