package com.mongodb.pipeline.transfer.test.operator.arithmetic;

import org.junit.Test;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/17     Create this file
 * </pre>
 */
public class IFNullTest {

    @Test
    public void sample1Test() {
        String json = "[\"$money\",\"\"]";


    }

    @Test
    public void sample2Test() {
        String json = "[ \"$ChannelFee\", NumberLong(\"0\") ]";


    }

}
