package com.mongodb.pipeline.transfer.constants;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class Constants {


    public static final String LPAREN = "(";

    public static final String RPAREN = ")";

    public static final String LBRACE = "{";

    public static final String RBRACE = "}";

    public static final String LBRACKET = "[";

    public static final String RBRACKET = "]";

    public static final String COMMA = ",";

    public static final String COLON = ":";

    public static final String APOSTROPHE = "'";

    /**
     * Type
     */
    public static final String DOLLAR = "$";

    public static final String NUMBER_INT = "NumberInt";

    public static final String NUMBER_LONG = "NumberLong";

    public static final String NUMBER_DECIMAL = "NumberDecimal";

    /**
     * convert 操作符
     */
    public static final String CONVERT_INPUT = "input";

    public static final String CONVERT_TO = "to";

    public static final String CONVERT_ON_ERROR = "onError";

    public static final String CONVERT_ON_NULL = "onNull";

    /**
     * cond 操作符
     */
    public static final String COND_IF = "if";

    public static final String COND_THEN = "then";

    public static final String COND_ELSE = "else";

    /**
     * switch 操作符
     */
    public static final String SWITCH_BRANCHES = "branches";

    public static final String SWITCH_DEFAULT = "default";

    public static final String SWITCH_CASE = "case";

    public static final String SWITCH_THEN = "then";

    /**
     * 日期通用 操作符
     */
    public static final String DATE_DATE = "date";

    public static final String DATE_TIMEZONE = "timezone";

    public static final String DATE_FORMAT = "format";

    public static final String DATE_ON_ERROR = "onError";

    public static final String DATE_ON_NULL = "onNull";

    /**
     * dateFromString 操作符
     */
    public static final String DATEFROMSTRING_DATESTRING = "dateString";

    /**
     * 字符串 操作符
     */
    public static final String STRING_INPUT = "input";
    public static final String STRING_CHARS = "chars";

    private Constants() {
    }
}
