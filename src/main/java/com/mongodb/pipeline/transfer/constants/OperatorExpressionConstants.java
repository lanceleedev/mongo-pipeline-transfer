package com.mongodb.pipeline.transfer.constants;

/**
 * 操作表达式常量
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class OperatorExpressionConstants {

    /**
     * Arithmetic Expression Operators, 16
     */
    public static final String ABS = "$abs";
    public static final String ADD = "$add";
    public static final String CEIL = "$ceil";
    public static final String DIVIDE = "$divide";
    public static final String EXP = "$exp";
    public static final String FLOOR = "$floor";
    public static final String LN = "$ln";
    public static final String LOG = "$log";
    public static final String LOG10 = "$log10";
    public static final String MOD = "$mod";
    public static final String MULTIPLY = "$multiply";
    public static final String POW = "$pow";
    public static final String ROUND = "$round";
    public static final String SQRT = "$sqrt";
    public static final String SUBTRACT = "$subtract";
    public static final String TRUNC = "$trunc";

    /**
     * Array Expression Operators, 5
     */
    public static final String ARRAY_ELEM_AT = "$arrayElemAt";
    public static final String ARRAY_TO_OBJECT = "$arrayToObject";
    public static final String CONCAT_ARRAYS = "$concatArrays";
    public static final String FILTER = "$filter";
    public static final String IN = "$in";
    public static final String INDEX_OF_ARRAY = "$indexOfArray";
    public static final String IS_ARRAY = "$isArray";
    public static final String MAP = "$map";
    public static final String OBJECT_TO_ARRAY = "$objectToArray";
    public static final String RANGE = "$range";
    public static final String REDUCE = "$reduce";
    public static final String REVERSE_ARRAY = "$reverseArray";
    public static final String SIZE = "$size";
    public static final String SLICE = "$slice";
    public static final String ZIP = "$zip";

    /**
     * Boolean Expression Operators
     */
    public static final String AND = "$and";
    public static final String NOT = "$not";
    public static final String OR = "$or";

    /**
     * Comparison Expression Operators
     */
    public static final String CMP = "$cmp";
    public static final String EQ = "$eq";
    public static final String GT = "$gt";
    public static final String GTE = "$gte";
    public static final String LT = "$lt";
    public static final String LTE = "$lte";
    public static final String NE = "$ne";

    /**
     * Conditional Expression Operators
     */
    public static final String COND = "$cond";
    public static final String IF_NULL = "$ifNull";
    public static final String SWITCH = "$switch";

    /**
     * Date Expression Operators
     */
    public static final String DATE_FROM_PARTS = "$dateFromParts";
    public static final String DATE_FROM_STRING = "$dateFromString";
    public static final String DATE_TO_PARTS = "$dateToParts";
    public static final String DATE_TO_STRING = "$dateToString";
    public static final String DAY_OF_MONTH = "$dayOfMonth";
    public static final String DAY_OF_WEEK = "$dayOfWeek";
    public static final String DAY_OF_YEAR = "$dayOfYear";
    public static final String HOUR = "$hour";
    public static final String ISO_DAY_OF_WEEK = "$isoDayOfWeek";
    public static final String ISO_WEEK = "$isoWeek";
    public static final String ISO_WEEK_YEAR = "$isoWeekYear";
    public static final String MILLISECOND = "$millisecond";
    public static final String MINUTE = "$minute";
    public static final String MONTH = "$month";
    public static final String SECOND = "$second";
    public static final String TO_DATE = "$toDate";
    public static final String WEEK = "$week";
    public static final String YEAR = "$year";

    /**
     * Literal Expresion Operator
     */
    public static final String LITERAL = "$literal";

    /**
     * Object Expression Operators
     */
    public static final String MERGE_OBJECTS = "$mergeObjects";

    /**
     * Set Expression Operators
     */
    public static final String ALL_ELEMENTS_TRUE = "$allElementsTrue";
    public static final String ANY_ELEMENT_TRUE = "$anyElementTrue";
    public static final String SET_DIFFERENCE = "$setDifference";
    public static final String SET_EQUALS = "$setEquals";
    public static final String SET_INTERSECTION = "$setIntersection";
    public static final String SET_IS_SUBSET = "$setIsSubset";
    public static final String SET_UNION = "$setUnion";

    /**
     * String Expression Operators
     */
    public static final String CONCAT = "$concat";
    public static final String INDEX_OF_BYTES = "$indexOfBytes";
    public static final String INDEX_OF_CP = "$indexOfCP";
    public static final String LTRIM = "$ltrim";
    public static final String REGEX_FIND = "$regexFind";
    public static final String REGEX_FIND_ALL = "$regexFindAll";
    public static final String REGEX_MATCH = "$regexMatch";
    public static final String RTRIM = "$rtrim";
    public static final String SPLIT = "$split";
    public static final String STR_LEN_BYTES = "$strLenBytes";
    public static final String STR_LEN_CP = "$strLenCP";
    public static final String STRCASECMP = "$strcasecmp";
    public static final String SUBSTR = "$substr";
    public static final String SUBSTR_BYTES = "$substrBytes";
    public static final String SUBSTR_CP = "$substrCP";
    public static final String TO_LOWER = "$toLower";
    public static final String TRIM = "$trim";
    public static final String TO_UPPER = "$toUpper";

    /**
     * Text Expression Operator
     */
    public static final String META = "$meta";

    /**
     * Trigonometry Expression Operators
     */
    public static final String SIN = "$sin";
    public static final String COS = "$cos";
    public static final String TAN = "$tan";
    public static final String ASIN = "$asin";
    public static final String ACOS = "$acos";
    public static final String ATAN = "$atan";
    public static final String ATAN2 = "$atan2";
    public static final String ASINH = "$asinh";
    public static final String ACOSH = "$acosh";
    public static final String ATANH = "$atanh";
    public static final String DEGREES_TO_RADIANS = "$degreesToRadians";
    public static final String RADIANS_TO_DEGREES = "$radiansToDegrees";

    /**
     * Type Expression Operators
     */
    public static final String CONVERT = "$convert";
    public static final String TO_BOOL = "$toBool";
    public static final String TO_DECIMAL = "$toDecimal";
    public static final String TO_DOUBLE = "$toDouble";
    public static final String TO_INT = "$toInt";
    public static final String TO_LONG = "$toLong";
    public static final String TO_OBJECT_ID = "$toObjectId";
    public static final String TO_STRING = "$toString";
    public static final String TYPE = "$type";

    /**
     * Accumulators
     */
    public static final String ADD_TO_SET = "$addToSet";
    public static final String AVG = "$avg";
    public static final String FIRST = "$first";
    public static final String LAST = "$last";
    public static final String MAX = "$max";
    public static final String MIN = "$min";
    public static final String PUSH = "$push";
    public static final String STD_DEV_POP = "$stdDevPop";
    public static final String STD_DEV_SAMP = "$stdDevSamp";
    public static final String SUM = "$sum";

    /**
     * Variable Expression Operators
     */
    public static final String LET = "$let";

    private OperatorExpressionConstants() {
    }
}
