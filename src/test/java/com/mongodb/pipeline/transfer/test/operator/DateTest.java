package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.client.model.Field;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.junit.Test;

/** <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public class DateTest {
    @Test
    public void dateFromStringTest() {
        String json = "{$dateFromString: {" +
                "dateString: \"$交易日期\"," +
                "format: \"%Y%m%d\"\n" +
                "}}";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);

        Document dateFromString = ExpressionHelper.parse(parseValue);
        System.out.println(dateFromString);

        Document result = new Document("$dateFromString", new Document("dateString",  "$交易日期").append("format","%Y%m%d"));
        System.out.println(result);
    }
}
