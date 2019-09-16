package com.mongodb.pipeline.transfer.constants;

/**
 * pipeline stage 常量
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/11     Create this file
 * </pre>
 */
public final class StagesConstants {
    /**
     * Mongo pipeline stage, 28
     */
    public static final String ADD_FIELDS = "$addFields";

    public static final String BUCKET = "$bucket";

    public static final String BUCKET_AUTO = "$bucketAuto";

    public static final String COLL_STATS = "$collStats";

    public static final String COUNT = "$count";

    public static final String FACET = "$facet";

    public static final String GEO_NEAR = "$geoNear";

    public static final String GRAPH_LOOKUP = "$graphLookup";

    public static final String GROUP = "$group";

    public static final String INDEX_STATS = "$indexStats";

    public static final String LIMIT = "$limit";

    public static final String LIST_SESSIONS = "$listSessions";

    public static final String LOOKUP = "$lookup";

    public static final String MATCH = "$match";

    public static final String MERGE = "$merge";

    public static final String OUT = "$out";

    public static final String PLAN_CACHE_STATS = "$planCacheStats";

    public static final String PROJECT = "$project";

    public static final String REDACT = "$redact";

    public static final String REPLACE_ROOT = "$replaceRoot";

    public static final String REPLACE_WITH = "$replaceWith";

    public static final String SAMPLE = "$sample";

    public static final String SET = "$set";

    public static final String SKIP = "$skip";

    public static final String SORT = "$sort";

    public static final String SORT_BY_COUNT = "$sortByCount";

    public static final String UNWIND = "$unwind";

    public static final String UNSET = "$unset";

    private StagesConstants() {
    }
}
