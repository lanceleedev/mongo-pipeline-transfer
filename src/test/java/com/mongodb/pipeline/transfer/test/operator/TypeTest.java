package com.mongodb.pipeline.transfer.test.operator;

import com.mongodb.pipeline.transfer.helper.ExpressionHelper;
import com.mongodb.pipeline.transfer.parse.operator.TypeExpressionOperators;
import com.mongodb.pipeline.transfer.util.JSONUtils;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/10/8     Create this file
 * </pre>
 */
public class TypeTest {
    @Test
    public void convertTest1() {
        String json = "{" +
                "         input: \"$qty\", to: \"int\"," +
                "         onError:{$concat:[\"Could not convert \", {$toString:\"$qty\"}, \" to type integer.\"]}," +
                "         onNull: NumberInt(\"0\")" +
                "      }";
        String parseValue = JSONUtils.fastjsonParsePreDeal(json);

        Document convert = ExpressionHelper.parse("$convert", parseValue);
        System.out.println(convert);


        Document result = new Document("$convert",
                new Document("input", "$qty")
                        .append("to","int")
                        .append("onError", new Document("$concat", Arrays.asList("Could not convert ", new Document("$toString", "$qty"), " to type integer.")))
                        .append("onNull", new BsonInt32(0))
                        );
        System.out.println(result);
    }

    @Test
    public void convertTest2() {
        String json = "{input: \"$qty\", to: \"int\"}";

        Document convert = ExpressionHelper.parse("$convert", json);
        System.out.println(convert);


//        Document result = new Document("$convert", new Document("input", "$qty").append("to","int").append("onError",""));
//        System.out.println(result);
    }
}
