package com.mongodb.pipeline.transfer.parse.operator;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.pipeline.transfer.constants.Constants;
import com.mongodb.pipeline.transfer.constants.OperatorExpressionConstants;
import org.bson.Document;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public final class TypeExpressionOperators extends Operators {
    private TypeExpressionOperators() {
    }

    /**
     * convert 操作符解析
     * {
     *    $convert:
     *       {
     *          input: <expression>,
     *          to: <type expression>,
     *          onError: <expression>,  // Optional.
     *          onNull: <expression>    // Optional.
     *       }
     * }
     *
     * @param json convert 内容
     * @return
     */
    public static Document convert(String json) {
        JSONObject obj = JSONObject.parseObject(json);
        String inputVal = obj.getString(Constants.CONVERT_INPUT);
        String toVal = obj.getString(Constants.CONVERT_TO);

        Document document = new Document(Constants.CONVERT_INPUT, getExpressionValue(inputVal)).append(Constants.CONVERT_TO, toVal);

        Object onErrorVal = obj.get(Constants.CONVERT_ON_ERROR);
        if (null != onErrorVal) {
            document.append(Constants.CONVERT_ON_ERROR, getExpressionValue(onErrorVal.toString()));
        }

        Object onNullVal = obj.get(Constants.CONVERT_ON_NULL);
        if (null != onNullVal) {
            document.append(Constants.CONVERT_ON_NULL, getExpressionValue(onNullVal.toString()));
        }

        return new Document(OperatorExpressionConstants.CONVERT, document);
    }
}
