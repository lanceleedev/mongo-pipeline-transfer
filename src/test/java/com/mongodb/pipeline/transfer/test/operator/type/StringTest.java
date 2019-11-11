package com.mongodb.pipeline.transfer.test.operator.type;

import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import org.bson.Document;
import org.junit.Test;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public class StringTest {
    @Test
    public void concatTest1() {

    }

    @Test
    public void toLowerTest1() {
        String json = "{$toLower : {$year: \"$dateTime\"}}";
        Document toLower = ExpressionHelper.parse(json);

        Document result = new Document("$toLower", new Document("$year", "$dateTime"));
    }

    @Test
    public void trimTest1() {
        String json = "{ $trim: { input: \"$description\" } }";
        Document trim = ExpressionHelper.parse(json);

        Document result = new Document("$trim", new Document("input", "$description"));
        System.out.println(trim);
        System.out.println(result);
    }






}
