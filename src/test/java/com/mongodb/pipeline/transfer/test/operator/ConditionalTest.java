package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.Arrays;

/** <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public class ConditionalTest {
    @Test
    public void switchTest() {
        String json = "{" +
                "branches: [" +
                "   {" +
                "       case: { $gte : [ { $avg : \"$Income\" }, 90 ] },\n" +
                "       then: \"Doing great!\"\n" +
                "   },\n" +
                "   {\n" +
                "       case: { $and : [ { $gte : [ { $avg : \"$Income\" }, 80 ] },\n" +
                "                { $lt : [ { $avg : \"$Income\" }, 90 ] } ] },\n" +
                "       then: \"Doing pretty well.\"\n" +
                "   },\n" +
                "   {\n" +
                "       case: { $lt : [ { $avg : \"$Income\" }, 80 ] },\n" +
                "       then: \"Needs improvement.\"\n" +
                "   }\n" +
                "   ],\n" +
                "default: \"No scores found.\"\n" +
                "}";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);

        Document convert = ExpressionHelper.parse("$switch", parseValue);
        System.out.println(convert);

        Document result = new Document("$switch",
                new Document("branches", Arrays.asList(
                        new Document("case", new Document("$gte", Arrays.asList(new Document("$avg", "$Income"), 90))).append("then", "Doing great!"),
                        new Document("case", new Document("$and", Arrays.asList(
                                new Document("$gte", Arrays.asList(new Document("$avg", "$Income"), 80)),
                                new Document("$lt", Arrays.asList(new Document("$avg", "$Income"), 90))
                        ))).append("then", "Doing pretty well."),
                        new Document("case", new Document("$lt", Arrays.asList(new Document("$avg", "$Income"), 80))).append("then", "Needs improvement.")))
                        .append("default", "No scores found."));
        System.out.println(result);

    }

    @Test
    public void condTest(){
        String json = "{$cond: {if: {$eq: [\"$MaxIncomeStatus\", NumberInt(\"10\")]}, " +
                "then: NumberInt(\"10\"), " +
                "else: {$cond: {if: {$gte: [\"$MaxIncomeStatus\", NumberInt(\"30\")]}, " +
                "   then: NumberInt(\"30\"), " +
                "   else: NumberInt(\"20\")}}}}";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);
        Document cond = ExpressionHelper.parse(parseValue);
        System.out.println(cond);
    }
}
