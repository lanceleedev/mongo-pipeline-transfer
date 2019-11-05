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
        Object formatVal = expObj.get(Constants.DATE_FORMAT);
        Object timezoneVal = expObj.get(Constants.DATE_TIMEZONE);
        Object onErrorVal = expObj.get(Constants.DATE_ON_ERROR);
        Object onNullVal = expObj.get(Constants.DATE_ON_NULL);

        Document docContent = new Document(Constants.DATEFROMSTRING_DATESTRING, getExpressionValue(dateStringVal));

        if (null != formatVal) {
            docContent.append(Constants.DATE_FORMAT, formatVal.toString());
        }

        if (null != timezoneVal) {
            docContent.append(Constants.DATE_TIMEZONE, getExpressionValue(timezoneVal.toString()));
        }

        if (null != onErrorVal) {
            docContent.append(Constants.DATE_ON_ERROR, getExpressionValue(onErrorVal.toString()));
        }

        if (null != onNullVal) {
            docContent.append(Constants.DATE_ON_NULL, getExpressionValue(onNullVal.toString()));
        }

        return new Document(OperatorExpressionConstants.DATE_FROM_STRING, docContent);
    }

    /**
     * $dateToString 操作符解析
     * <code>
     * { $dateToString: {
     *     date: <dateExpression>,
     *     format: <formatString>,
     *     timezone: <tzExpression>, // Optional.
     *     onNull: <expression>      // Optional.
     * } }
     * </code>
     * @param json 操作符内容
     * @return
     */
    public static Document dateToString(String json) {
        JSONObject expObj = JSONObject.parseObject(json);
        String dateVal = expObj.getString(Constants.DATE_DATE);
        Object formatVal = expObj.get(Constants.DATE_FORMAT);
        Object timezoneVal = expObj.get(Constants.DATE_TIMEZONE);
        Object onNullVal = expObj.get(Constants.DATE_ON_NULL);

        Document docContent = new Document(Constants.DATE_DATE, getExpressionValue(dateVal)).append(Constants.DATE_FORMAT, formatVal.toString());

        if (null != timezoneVal) {
            docContent.append(Constants.DATE_TIMEZONE, getExpressionValue(timezoneVal.toString()));
        }

        if (null != onNullVal) {
            docContent.append(Constants.DATE_ON_NULL, getExpressionValue(onNullVal.toString()));
        }

        return new Document(OperatorExpressionConstants.DATE_TO_STRING, docContent);
    }

    /**
     * $year 操作符解析。支持2中格式
     * <code>
     * { $year: <dateExpression> }
     * or
     * { $year: {
     *      date: <dateExpression>,
     *      timezone: <tzExpression>  // Optional.
     * } }
     * </code>
     * @param json 操作符内容
     * @return
     */
    public static Document year(String json) {
        if (json.startsWith(Constants.LBRACE)) {
            JSONObject expObj = JSONObject.parseObject(json);
            String dateVal = expObj.getString(Constants.DATE_DATE);
            Object timezoneVal = expObj.get(Constants.DATE_TIMEZONE);
            Document docContent = new Document(Constants.DATE_DATE, getExpressionValue(dateVal));

            if (null != timezoneVal) {
                docContent.append(Constants.DATE_TIMEZONE, getExpressionValue(timezoneVal.toString()));
            }

            return new Document(OperatorExpressionConstants.YEAR, docContent);
        }
        return new Document(OperatorExpressionConstants.YEAR, getExpressionValue(json));
    }

    /**
     * $month 操作符解析。支持2中格式
     * <code>
     * { $month: <dateExpression> }
     * or
     * { $month: {
     *      date: <dateExpression>,
     *      timezone: <tzExpression>  // Optional.
     * } }
     * </code>
     * @param json 操作符内容
     * @return
     */
    public static Document month(String json) {
        if (json.startsWith(Constants.LBRACE)) {
            JSONObject expObj = JSONObject.parseObject(json);
            String dateVal = expObj.getString(Constants.DATE_DATE);
            Object timezoneVal = expObj.get(Constants.DATE_TIMEZONE);
            Document docContent = new Document(Constants.DATE_DATE, getExpressionValue(dateVal));

            if (null != timezoneVal) {
                docContent.append(Constants.DATE_TIMEZONE, getExpressionValue(timezoneVal.toString()));
            }

            return new Document(OperatorExpressionConstants.MONTH, docContent);
        }
        return new Document(OperatorExpressionConstants.MONTH, getExpressionValue(json));

    }
}
