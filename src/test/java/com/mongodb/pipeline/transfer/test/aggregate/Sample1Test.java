package com.mongodb.pipeline.transfer.test.aggregate;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.pipeline.transfer.MongoDBPipelineUtils;
import com.mongodb.pipeline.transfer.test.util.FileUtils;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/17     Create this file
 * </pre>
 */
public class Sample1Test {
    @Test
    public void SampleTest() {
        MongoClient mongoClient = new MongoClient("172.31.1.41", 27017);
        MongoDatabase database = mongoClient.getDatabase("mongodb4datacenter");

        List<Bson> parseBson = getParseBson();
        System.out.println(parseBson);
        List<Bson> compareBson = getCompareBson();


        MongoCollection<Document> collection = database.getCollection("YH_Gathering");
        MongoCursor<Document> dbCursor = collection.aggregate(parseBson).allowDiskUse(true).iterator();
        int count = 0;
        while (dbCursor.hasNext()) {
            Document item = dbCursor.next();
            count++;
            System.out.println(item.toJson());
        }
        System.out.println(count);
        dbCursor.close();
        mongoClient.close();
    }

    private List<Bson> getParseBson() {
        String path = this.getClass().getResource("/").getPath();
        String resourcePath = path.substring(0, path.indexOf("target"));
        String aggregateJson = FileUtils.readFileToString(resourcePath + "/src/test/resource/PipelineTestFile/1-Sample1.js");
        return MongoDBPipelineUtils.aggregateJson2BsonList(aggregateJson);
    }

    private List<Bson> getCompareBson() {
        List<Bson> list = new ArrayList<>();
        list.add(Aggregates.match(Filters.and(Filters.gte("Status", 20), Filters.lte("Status", 30), Filters.gte("BankResponseTime", "20171017152400"),
                Filters.lt("BankResponseTime", "20171017164640"))));

        list.add(Aggregates.group(
                new Document("Status", "$Status").append("GatheringWay", "$GatheringWay").append("ResponseCode", "$ResponseCode")
                        .append("InstitutionID", "$InstitutionID").append("SourceTxType", "$SourceTxType").append("IssueBankID", "$IssueBankID")
                        .append("ResponseMessage", "$ResponseMessage").append("MerchantID", "$MerchantID").append("FinanceInstCode", "$FinanceInstCode")
                        .append("AccountType", "$AccountType").append("BankResponseCode", "$BankResponseCode").append("MerchantName", "$MerchantName")
                        .append("CardType", "$CardType").append("BankResponseMessage", "$BankResponseMessage")
                        .append("日期过度", new Document("$substr", Arrays.asList("$BankResponseTime", 0, 8))), Accumulators.sum("交易笔数", 1),
                Accumulators.sum("交易金额", "$Amount")));

        List<Variable<String>> variables = Arrays.asList(new Variable<>("g_txType", "$_id.SourceTxType"));
        List<Bson> pipeline = Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", Arrays.asList(
                new Document("$eq", Arrays.asList("$BusinessDescription", "代收")), new Document("$eq", Arrays.asList("$TxType", "$$g_txType"))))))));
        list.add(Aggregates.lookup("JC_TypeInfo", variables, pipeline, "resultdoc"));

        List<Variable<String>> variables2 = Arrays.asList(new Variable<>("g_InstitutionID", "$_id.InstitutionID"));
        List<Bson> pipeline2 = Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList(
                "$InstitutionID", "$$g_InstitutionID"))))))));
        list.add(Aggregates.lookup("JC_Institution", variables2, pipeline2, "resultdoc2"));

        List<Variable<String>> variables4 = Arrays.asList(new Variable<>("g_IssueBankID", "$_id.IssueBankID"));
        List<Bson> pipeline4 = Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList(
                "$BankID", "$$g_IssueBankID"))))))));
        list.add(Aggregates.lookup("JC_BankInfo", variables4, pipeline4, "resultdoc4"));

        List<Variable<String>> variables5 = Arrays.asList(new Variable<>("g_FinanceInstCode", "$_id.FinanceInstCode"));
        List<Bson> pipeline5 = Arrays.asList(Aggregates.match(new Document("$expr", (new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList(
                "$BankID", "$$g_FinanceInstCode"))))))));
        list.add(Aggregates.lookup("JC_BankInfo", variables5, pipeline5, "resultdoc5"));

        list.add(Aggregates.group(
                new Document("状态", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.Status", 20)), "成功", "失败")))
                        .append("银行ID", "$_id.IssueBankID")
                        .append("银行响应消息", "$_id.BankResponseMessage")
                        .append("中金响应消息", "$_id.ResponseMessage")
                        .append("通道ID", "$_id.FinanceInstCode")
                        .append("一级机构名称", "$resultdoc2.InstitutionParentName")
                        .append("交易类型说明", "$resultdoc.TxTypeDescription")
                        .append("所属区域", new Document("$ifNull", Arrays.asList("$resultdoc2.Area", "")))
                        .append("商户所属", new Document("$ifNull", Arrays.asList("$resultdoc2.BusinessArea", "")))
                        .append("银行响应代码", "$_id.BankResponseCode")
                        .append("机构ID", "$_id.InstitutionID")
                        .append("通道名称", "$resultdoc5.BankName")
                        .append("二级商户号", "$_id.MerchantID")
                        .append("银行名称", "$resultdoc4.BankName")
                        .append("业务经理", "$resultdoc2.MarketingManager")
                        .append("卡类型",
                                new Document("$cond", Arrays.asList(
                                        new Document("$eq", Arrays.asList("$_id.CardType", "10")),
                                        "借记卡",
                                        new Document("$cond", Arrays.asList(
                                                new Document("$eq", Arrays.asList("$_id.CardType", "20")),
                                                "贷记卡",
                                                new Document("$cond", Arrays.asList(
                                                        new Document("$eq", Arrays.asList("$_id.CardType", "30")),
                                                        "存折",
                                                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.CardType", "40")), "电子账户",
                                                                "")))))))))
                        .append("机构创建时间", new Document("$ifNull", Arrays.asList("$resultdoc2.CreateTime", "20150101")))
                        .append("产品分类", "$resultdoc.ProductCategory")
                        .append("二级商户名称", "$_id.MerchantName")
                        .append("实际业务类型",
                                new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.GatheringWay", "11")), "单笔代收", new Document(
                                        "$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.GatheringWay", "12")), "批量代收", "")))))
                        .append("账户类型",
                                new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.AccountType", "11")), "个人", new Document("$cond",
                                        Arrays.asList(new Document("$eq", Arrays.asList("$_id.AccountType", "12")), "企业", "")))))
                        .append("交易类型代码", "$_id.SourceTxType").append("业务类型", "$resultdoc.BusinessDescription")
                        .append("商服人员", new Document("$ifNull", Arrays.asList("$resultdoc2.BusinessServices", "")))
                        .append("机构名称", "$resultdoc2.InstitutionName").append("技术支持", new Document("$ifNull", Arrays.asList("$resultdoc2.CPCNSupports", "")))
                        .append("一级机构ID", "$resultdoc2.InstitutionParentID").append("所属行业", new Document("$ifNull", Arrays.asList("$resultdoc2.Trade", "")))
                        .append("机构简称", "$resultdoc2.InstitutionShortName")
                        .append("机构复核时间", new Document("$ifNull", Arrays.asList("$resultdoc2.CheckTime", "20150101"))).append("中金响应代码", "$_id.ResponseCode")
                        .append("日期过度", "$_id.日期过度"), Accumulators.sum("实际交易金额分",
                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.Status", 20)), "$交易金额", 0))), Accumulators.sum("总交易金额分",
                        "$交易金额"), Accumulators.sum("失败交易笔数",
                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.Status", 30)), "$交易笔数", 0))), Accumulators.sum("总交易笔数",
                        "$交易笔数"), Accumulators.sum("实际交易笔数",
                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.Status", 20)), "$交易笔数", 0))), Accumulators.sum("失败交易金额分",
                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$_id.Status", 30)), "$交易金额", 0)))));

        list.add(Aggregates.unwind("$_id.交易类型代码", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.业务类型", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.产品分类", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.交易类型说明", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.业务类型", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.银行名称", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.通道名称", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.业务类型", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.机构名称", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.机构简称", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.一级机构ID", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.一级机构名称", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.业务经理", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.机构创建时间", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.机构复核时间", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.所属区域", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.所属行业", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.商服人员", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.技术支持", new UnwindOptions().preserveNullAndEmptyArrays(true)));
        list.add(Aggregates.unwind("$_id.商户所属", new UnwindOptions().preserveNullAndEmptyArrays(true)));

        list.add(Aggregates.project(new Document("_id", "0").append("状态", new Document("$ifNull", Arrays.asList("$_id.状态", "")))
                .append("日期过度", new Document("$ifNull", Arrays.asList("$_id.日期过度", "")))
                .append("交易类型代码", new Document("$ifNull", Arrays.asList("$_id.交易类型代码", "")))
                .append("机构ID", new Document("$ifNull", Arrays.asList("$_id.机构ID", "")))
                .append("业务类型", new Document("$ifNull", Arrays.asList("$_id.业务类型", "")))
                .append("产品分类", new Document("$ifNull", Arrays.asList("$_id.产品分类", "")))
                .append("交易类型说明", new Document("$ifNull", Arrays.asList("$_id.交易类型说明", "")))
                .append("实际业务类型", new Document("$ifNull", Arrays.asList("$_id.实际业务类型", "")))
                .append("银行ID", new Document("$ifNull", Arrays.asList("$_id.银行ID", "")))
                .append("银行名称", new Document("$ifNull", Arrays.asList("$_id.银行名称", "")))
                .append("通道ID", new Document("$ifNull", Arrays.asList("$_id.通道ID", "")))
                .append("通道名称", new Document("$ifNull", Arrays.asList("$_id.通道名称", ""))).append("卡类型", new Document("$ifNull", Arrays.asList("$_id.卡类型", "")))
                .append("账户类型", new Document("$ifNull", Arrays.asList("$_id.账户类型", "")))
                .append("中金响应代码", new Document("$ifNull", Arrays.asList("$_id.中金响应代码", "")))
                .append("中金响应消息", new Document("$ifNull", Arrays.asList("$_id.中金响应消息", "")))
                .append("银行响应代码", new Document("$ifNull", Arrays.asList("$_id.银行响应代码", "")))
                .append("银行响应消息", new Document("$ifNull", Arrays.asList("$_id.银行响应消息", "")))
                .append("二级商户号", new Document("$ifNull", Arrays.asList("$_id.二级商户号", "")))
                .append("二级商户名称", new Document("$ifNull", Arrays.asList("$_id.二级商户名称", "")))
                .append("机构名称", new Document("$ifNull", Arrays.asList("$_id.机构名称", "")))
                .append("机构简称", new Document("$ifNull", Arrays.asList("$_id.机构简称", "")))
                .append("一级机构ID", new Document("$ifNull", Arrays.asList("$_id.一级机构ID", "")))
                .append("一级机构名称", new Document("$ifNull", Arrays.asList("$_id.一级机构名称", "")))
                .append("业务经理", new Document("$ifNull", Arrays.asList("$_id.业务经理", "")))
                .append("机构创建时间", new Document("$ifNull", Arrays.asList("$_id.机构创建时间", "")))
                .append("机构复核时间", new Document("$ifNull", Arrays.asList("$_id.机构复核时间", "")))
                .append("所属区域", new Document("$ifNull", Arrays.asList("$_id.所属区域", "")))
                .append("所属行业", new Document("$ifNull", Arrays.asList("$_id.所属行业", "")))
                .append("商服人员", new Document("$ifNull", Arrays.asList("$_id.商服人员", "")))
                .append("技术支持", new Document("$ifNull", Arrays.asList("$_id.技术支持", "")))
                .append("商户所属", new Document("$ifNull", Arrays.asList("$_id.商户所属", ""))).append("商户所属", "$_id.机构ID")
                .append("实际交易金额分", new Document("$multiply", Arrays.asList("$实际交易金额分", "$numberLong(1)"))).append("实际交易笔数", 1)
                .append("失败交易金额分", new Document("$multiply", Arrays.asList("$失败交易金额分", "$numberLong(1)"))).append("失败交易笔数", 1)
                .append("总交易金额分", new Document("$multiply", Arrays.asList("$总交易金额分")))
                .append("总交易金额分test", new Document("$multiply", Arrays.asList("$总交易金额分", new BsonInt64(1)))).append("总交易笔数", 1)));
        return list;
    }
}
