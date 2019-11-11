package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.client.model.Field;
import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.junit.Test;

import java.util.Date;

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

    @Test
    public void yearTest() {
        String json = "{ $year: { $dateFromString: { dateString: \"$交易日期\", format: \"%Y%m%d\" } }}";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);

        Document year = ExpressionHelper.parse(parseValue);
        System.out.println(year);

        Document result = new Document("$year", new Document("$dateFromString", new Document("dateString",  "$交易日期").append("format","%Y%m%d")));
        System.out.println(result);
    }


    @Test
    public void dayOfMonthTest1() {
        String json = "{ $dayOfMonth: new Date(\"2016-01-01\") }";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);

        Document dayOfMonth = ExpressionHelper.parse(parseValue);
        System.out.println(dayOfMonth);

        Document result = new Document("$dayOfMonth", "new Date(\"2016-01-01\")");
        System.out.println(result);
    }

    @Test
    public void dayOfMonthTest2() {
        String json = "{ $dayOfMonth: {\n" +
                "    date: new Date(\"August 14, 2011\"),\n" +
                "    timezone: \"America/Chicago\"\n" +
                "} }";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);

        Document dayOfMonth = ExpressionHelper.parse(parseValue);
        System.out.println(dayOfMonth);

        Document result = new Document("$dayOfMonth", new Document("date",  "new Date(\"August 14, 2011\")").append("timezone","America/Chicago"));
        System.out.println(result);
    }

}
