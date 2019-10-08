package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import org.bson.Document;

/**
 * 日期操作
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class DateExpressionOperators extends Operators {
    private DateExpressionOperators() {
    }

    /**
     * $dateFromString 操作符解析
     * <code>
     * { $dateFromString: {
     *     dateString: <dateStringExpression>,
     *     format: <formatStringExpression>,  // Optional.
     *     timezone: <tzExpression>,          // Optional.
     *     onError: <onErrorExpression>,      // Optional.
     *     onNull: <onNullExpression>         // Optional.
     * } }
     * </code>
     * @param json 操作符内容
     * @return
     */
    public static Document dateFromString(String json) {
        JSONObject expObj = JSONObject.parseObject(json);
        String dateStringVal = expObj.getString(Constants.DATEFROMSTRING_DATESTRING);
        Object formatVal = expObj.get(Constants.DATEFROMSTRING_FORMAT);
        Object timezoneVal = expObj.get(Constants.DATEFROMSTRING_TIMEZONE);
        Object onErrorVal = expObj.get(Constants.DATEFROMSTRING_ON_ERROR);
        Object onNullVal = expObj.get(Constants.DATEFROMSTRING_ON_NULL);

        Document docContent = new Document(Constants.DATEFROMSTRING_DATESTRING, getExpressionValue(dateStringVal));

        if (null != formatVal) {
            docContent.append(Constants.DATEFROMSTRING_FORMAT, formatVal.toString());
        }

        if (null != timezoneVal) {
            docContent.append(Constants.DATEFROMSTRING_TIMEZONE, getExpressionValue(timezoneVal.toString()));
        }

        if (null != onErrorVal) {
            docContent.append(Constants.DATEFROMSTRING_ON_ERROR, getExpressionValue(onErrorVal.toString()));
        }

        if (null != onNullVal) {
            docContent.append(Constants.DATEFROMSTRING_ON_NULL, getExpressionValue(onNullVal.toString()));
        }

        return new Document(OperatorExpressionConstants.DATE_FROM_STRING, docContent);
    }
}
