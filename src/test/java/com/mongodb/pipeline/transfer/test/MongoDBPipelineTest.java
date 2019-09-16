/**
 * 版权所有 (c) 2018，中金支付有限公司  
 */
package com.mongodb.pipeline.transfer.test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.pipeline.transfer.MongoDBPipelineUtils;
import com.mongodb.pipeline.transfer.test.util.FileUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.List;

/**
 * TODO
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019年6月12日           Create this file
 * </pre>
 */
public class MongoDBPipelineTest {

    @Test
    public void codeTest() {
        MongoClient mongoClient = new MongoClient("172.31.1.41", 27017);
        MongoDatabase database = mongoClient.getDatabase("mongodb4datacenter");

        mongoClient.close();
    }

    public void sample1Test(MongoDatabase database) {
        MongoCollection<Document> collection = database.getCollection("HJ_CollectedTx");
        List<Bson> bsonList = MongoDBPipelineUtils.aggregateJson2BsonList(getHJ_CollectedTxAggregate());
        for (Bson bson : bsonList) {
            System.out.println(bson);
        }

        query(collection, bsonList);
    }



    public void sample2Test(MongoDatabase database) {
        List<Bson> bsonList = MongoDBPipelineUtils.aggregateJson2BsonList(getYH_GatheringAggregate());
        for (Bson bson : bsonList) {
            System.out.println(bson);
        }
        // query(collection, bsonList);
    }

    public void sample3Test(MongoDatabase database) {
        List<Bson> bsonList = MongoDBPipelineUtils.aggregateJson2BsonList(getCollectedTx());
        for (Bson bson : bsonList) {
            System.out.println(bson);
        }

    }



    public String getHJ_CollectedTxAggregate() {
        return "[{$match:{TxDate:{$gte:\"20151001\",$lt:\"20151002\"}}},{$group:{_id:{TxDate:\"$TxDate\",InstitutionID:\"$InstitutionID\",TxType:\"$TxType\",BankID:\"$BankID\",BankOrderType:{$cond:{if:{$eq:[\"$BankOrderType\",21]},then:\"个人\",else:{$cond:{if:{$eq:[\"$BankOrderType\",22]},then:\"企业\",else:\"其它\"}}}}},TxAmount:{$sum:\"$TxAmount\"},InstitutionAmount:{$sum:\"$InstitutionAmount\"},PaymentAmount:{$sum:\"$PaymentAmount\"},PayerFee:{$sum:\"$PayerFee\"},Fee:{$sum:\"$Fee\"},PaymentAccountFee:{$sum:\"$PaymentAccountFee\"},ChannelFee:{$sum:\"$ChannelFee\"},SettlementAmount:{$sum:\"$SettlementAmount\"},TxNum:{$sum:\"$Tcount\"}}},{$lookup:{from:\"HJ_TypeInfo\",let:{g_txType:\"$_id.TxType\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$TxType\",\"$$g_txType\"]}]}}}],as:\"resultdoc\"}},{$lookup:{from:\"JC_Institution\",let:{g_InstitutionID:\"$_id.InstitutionID\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$InstitutionID\",\"$$g_InstitutionID\"]}]}}}],as:\"resultdoc2\"}},{$lookup:{from:\"JC_BankInfo\",let:{g_IssueBankID:\"$_id.IssueBankID\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$BankID\",\"$$g_IssueBankID\"]}]}}}],as:\"resultdoc4\"}},{$group:{_id:{TxType:\"$_id.TxType\",TxTypeDescription:\"$resultdoc.TxTypeDescription\",ProductCategory:\"$resultdoc.ProductCategory\",Flag:\"$resultdoc.Flag\",TxDate:\"$_id.TxDate\",InstitutionID:\"$_id.InstitutionID\",InstitutionName:\"$resultdoc2.InstitutionName\",BankID:\"$_id.BankID\",BankName:{$cond:{if:{$eq:[\"$_id.BankID\",\"000\"]},then:\"通用设置\",else:\"$resultdoc4.BankName\"}},BankOrderType:\"$_id.BankOrderType\",MarketingManager:\"$resultdoc2.MarketingManager\",InstitutionCreateTime:{$ifNull:[\"$resultdoc2.CreateTime\",\"20150101\"]},InstitutionCheckTime:{$ifNull:[\"$resultdoc2.CheckTime\",\"20150101\"]},Area:\"$resultdoc2.Trade\",Trade:\"$resultdoc2.Area\"},TxAmount:{$sum:\"$TxAmount\"},InstitutionAmount:{$sum:\"$InstitutionAmount\"},PaymentAmount:{$sum:\"$PaymentAmount\"},PayerFee:{$sum:\"$PayerFee\"},Fee:{$sum:\"$Fee\"},PaymentAccountFee:{$sum:\"$PaymentAccountFee\"},ChannelFee:{$sum:\"$ChannelFee\"},SettlementAmount:{$sum:\"$SettlementAmount\"},TxNum:{$sum:\"$TxNum\"}}},{$unwind:{path:\"$_id.TxTypeDescription\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.ProductCategory\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.Flag\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BankName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.MarketingManager\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionCreateTime\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionCheckTime\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.Area\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.Trade\",preserveNullAndEmptyArrays:true}},{$project:{_id:0,ProductCategory:{$ifNull:[\"$_id.ProductCategory\",\"\"]},InstitutionID:{$ifNull:[\"$_id.InstitutionID\",\"\"]},InstitutionCreateTime:{$substr:[{$ifNull:[\"$_id.InstitutionCreateTime\",\"20150101\"]},0,8]},InstitutionCheckTime:{$substr:[{$ifNull:[\"$_id.InstitutionCheckTime\",\"20150101\"]},0,8]},InstitutionName:{$ifNull:[\"$_id.InstitutionName\",\"\"]},TxType:{$ifNull:[\"$_id.TxType\",\"\"]},TxTypeDescription:{$ifNull:[\"$_id.TxTypeDescription\",\"\"]},Area:{$ifNull:[\"$_id.Area\",\"\"]},Trade:{$ifNull:[\"$_id.Trade\",\"\"]},Flag:{$cond:{if:{$eq:[\"$_id.Flag\",\"10\"]},then:\"是\",else:\"否\"}},MarketingManager:{$ifNull:[\"$_id.MarketingManager\",\"\"]},BankID:{$ifNull:[\"$_id.BankID\",\"\"]},BankOrderType:{$ifNull:[\"$_id.BankOrderType\",\"\"]},BankName:{$ifNull:[\"$_id.BankName\",\"\"]},TxNum:1,PayerFee:1,InstitutionAmount:1,TxAmount:1,TxDate:{$ifNull:[\"$_id.TxDate\",\"\"]},SettlementAmount:1,ChannelFee:1,Fee:1,PaymentAmount:1,PaymentAccountFee:1}}]";
    }

    public String getYH_GatheringAggregate() {
        return "[{$match:{BankResponseTime:{$gte:\"20170919000000\",$lt:\"20171018000000\"},Status:{$gte:20,$lte:30}}},{$group:{_id:{BankResponseTime:{$substr:[\"$BankResponseTime\",0,8]},InstitutionID:\"$InstitutionID\",SourceTxType:\"$SourceTxType\",AccountType:\"$AccountType\",IssueBankID:\"$IssueBankID\",CardType:\"$CardType\",FinanceInstCode:\"$FinanceInstCode\",GatheringWay:\"$GatheringWay\",ResponseCode:\"$ResponseCode\",ResponseMessage:\"$ResponseMessage\",BankResponseCode:\"$BankResponseCode\",BankResponseMessage:\"$BankResponseMessage\",Status:\"$Status\",MerchantID:\"$MerchantID\",MerchantName:\"$MerchantName\"},交易金额:{$sum:\"$Amount\"},交易笔数:{$sum:1}}},{$lookup:{from:\"JC_TypeInfo\",let:{g_txType:\"$_id.SourceTxType\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$BusinessDescription\",\"代收\"]},{$eq:[\"$TxType\",\"$$g_txType\"]}]}}}],as:\"resultdoc\"}},{$lookup:{from:\"JC_Institution\",let:{g_InstitutionID:\"$_id.InstitutionID\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$InstitutionID\",\"$$g_InstitutionID\"]}]}}}],as:\"resultdoc2\"}},{$lookup:{from:\"JC_BankInfo\",let:{g_IssueBankID:\"$_id.IssueBankID\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$BankID\",\"$$g_IssueBankID\"]}]}}}],as:\"resultdoc4\"}},{$lookup:{from:\"JC_BankInfo\",let:{g_FinanceInstCode:\"$_id.FinanceInstCode\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$BankID\",\"$$g_FinanceInstCode\"]}]}}}],as:\"resultdoc5\"}},{$group:{_id:{Status:{$cond:{if:{$eq:[\"$_id.Status\",20]},then:\"成功\",else:\"失败\"}},BankResponseTime:\"$_id.BankResponseTime\",SourceTxType:\"$_id.SourceTxType\",InstitutionID:\"$_id.InstitutionID\",BusinessType:\"$resultdoc.BusinessDescription\",ProductCategory:\"$resultdoc.ProductCategory\",TxTypeDescription:\"$resultdoc.TxTypeDescription\",RealBusinessType:{$cond:{if:{$eq:[\"$_id.GatheringWay\",\"11\"]},then:\"单笔代收\",else:{$cond:{if:{$eq:[\"$_id.GatheringWay\",\"12\"]},then:\"批量代收\",else:\"\"}}}},BankID:\"$_id.IssueBankID\",BankName:\"$resultdoc4.BankName\",ChannelID:\"$_id.FinanceInstCode\",ChannelName:\"$resultdoc5.BankName\",CardType:{$cond:{if:{$eq:[\"$_id.CardType\",\"10\"]},then:\"借记卡\",else:{$cond:{if:{$eq:[\"$_id.CardType\",\"20\"]},then:\"贷记卡\",else:{$cond:{if:{$eq:[\"$_id.CardType\",\"30\"]},then:\"存折\",else:{$cond:{if:{$eq:[\"$_id.CardType\",\"40\"]},then:\"电子账户\",else:\"\"}}}}}}}},AccountType:{$cond:{if:{$eq:[\"$_id.AccountType\",11]},then:\"个人\",else:{$cond:{if:{$eq:[\"$_id.AccountType\",12]},then:\"企业\",else:\"\"}}}},ResponseCode:\"$_id.ResponseCode\",ResponseMessage:\"$_id.ResponseMessage\",BankResponseCode:\"$_id.BankResponseCode\",BankResponseMessage:\"$_id.BankResponseMessage\",SubMerchantID:\"$_id.MerchantID\",SubMerchantName:\"$_id.MerchantName\",InstitutionName:\"$resultdoc2.InstitutionName\",InstitutionShortName:\"$resultdoc2.InstitutionShortName\",InstitutionParentID:\"$resultdoc2.InstitutionParentID\",InstitutionParentName:\"$resultdoc2.InstitutionParentName\",MarketingManager:\"$resultdoc2.MarketingManager\",InstitutionCreateTime:{$ifNull:[\"$resultdoc2.CreateTime\",\"20150101\"]},InstitutionCheckTime:{$ifNull:[\"$resultdoc2.CheckTime\",\"20150101\"]},Area:{$ifNull:[\"$resultdoc2.Area\",\"\"]},Trade:{$ifNull:[\"$resultdoc2.Trade\",\"\"]},BusinessServices:{$ifNull:[\"$resultdoc2.BusinessServices\",\"\"]},CPCNSupports:{$ifNull:[\"$resultdoc2.CPCNSupports\",\"\"]},BusinessArea:{$ifNull:[\"$resultdoc2.BusinessArea\",\"\"]}},RealTxAmount:{$sum:{$cond:{if:{$eq:[\"$_id.Status\",20]},then:\"$交易金额\",else:0}}},RealTxNum:{$sum:{$cond:{if:{$eq:[\"$_id.Status\",20]},then:\"$交易笔数\",else:0}}},FailTxAmount:{$sum:{$cond:{if:{$eq:[\"$_id.Status\",30]},then:\"$交易金额\",else:0}}},FailTxNum:{$sum:{$cond:{if:{$eq:[\"$_id.Status\",30]},then:\"$交易笔数\",else:0}}},TotalTxAmount:{$sum:\"$交易金额\"},TotalTxNum:{$sum:\"$交易笔数\"}}},{$unwind:{path:\"$_id.SourceTxType\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BusinessType\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.ProductCategory\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.TxTypeDescription\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BusinessType\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BankName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.ChannelName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BusinessType\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionShortName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionParentID\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionParentName\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.MarketingManager\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionCreateTime\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.InstitutionCheckTime\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.Area\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.Trade\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BusinessServices\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.CPCNSupports\",preserveNullAndEmptyArrays:true}},{$unwind:{path:\"$_id.BusinessArea\",preserveNullAndEmptyArrays:true}},{$project:{_id:0,Status:{$ifNull:[\"$_id.Status\",\"\"]},BankResponseTime:{$ifNull:[\"$_id.BankResponseTime\",\"\"]},SourceTxType:{$ifNull:[\"$_id.SourceTxType\",\"\"]},InstitutionID:{$ifNull:[\"$_id.InstitutionID\",\"\"]},BusinessType:{$ifNull:[\"$_id.BusinessType\",\"\"]},ProductCategory:{$ifNull:[\"$_id.ProductCategory\",\"\"]},TxTypeDescription:{$ifNull:[\"$_id.TxTypeDescription\",\"\"]},RealBusinessType:{$ifNull:[\"$_id.RealBusinessType\",\"\"]},BankID:{$ifNull:[\"$_id.BankID\",\"\"]},BankName:{$ifNull:[\"$_id.BankName\",\"\"]},ChannelID:{$ifNull:[\"$_id.ChannelID\",\"\"]},ChannelName:{$ifNull:[\"$_id.ChannelName\",\"\"]},CardType:{$ifNull:[\"$_id.CardType\",\"\"]},AccountType:{$ifNull:[\"$_id.AccountType\",\"\"]},ResponseCode:{$ifNull:[\"$_id.ResponseCode\",\"\"]},ResponseMessage:{$ifNull:[\"$_id.ResponseMessage\",\"\"]},BankResponseCode:{$ifNull:[\"$_id.BankResponseCode\",\"\"]},BankResponseMessage:{$ifNull:[\"$_id.BankResponseMessage\",\"\"]},SubMerchantID:{$ifNull:[\"$_id.SubMerchantID\",\"\"]},SubMerchantName:{$ifNull:[\"$_id.SubMerchantName\",\"\"]},InstitutionName:{$ifNull:[\"$_id.InstitutionName\",\"\"]},InstitutionShortName:{$ifNull:[\"$_id.InstitutionShortName\",\"\"]},InstitutionParentID:{$ifNull:[\"$_id.InstitutionParentID\",\"\"]},InstitutionParentName:{$ifNull:[\"$_id.InstitutionParentName\",\"\"]},MarketingManager:{$ifNull:[\"$_id.MarketingManager\",\"\"]},InstitutionCreateTime:{$substr:[{$ifNull:[\"$_id.InstitutionCreateTime\",\"20150101\"]},0,8]},InstitutionCheckTime:{$substr:[{$ifNull:[\"$_id.InstitutionCheckTime\",\"20150101\"]},0,8]},Area:{$ifNull:[\"$_id.Area\",\"\"]},Trade:{$ifNull:[\"$_id.Trade\",\"\"]},BusinessServices:{$ifNull:[\"$_id.BusinessServices\",\"\"]},CPCNSupports:{$ifNull:[\"$_id.CPCNSupports\",\"\"]},BusinessArea:{$ifNull:[\"$_id.BusinessArea\",\"\"]},RealTxAmount:{\"$multiply\":[\"$RealTxAmount\",{\"$numberLong\":\"1\"}]},RealTxNum:1,FailTxAmount:{\"$multiply\":[\"$FailTxAmount\",{\"$numberLong\":\"1\"}]},FailTxNum:1,TotalTxAmount:{\"$multiply\":[\"$TotalTxAmount\",{\"$numberLong\":\"1\"}]},TotalTxNum:1,SourceTable:\"YH_Gathering\"}}]";
    }

    public String getCollectedTx() {
        return "[{$match:{TxDate:{$gte:\"20181112\",$lt:\"2018113\"}}},{$lookup:{from:\"SY_ClearingTxTmp\",let:{g_InstitutionID:\"$InstitutionID\",g_TxSn:\"$TxSn\",g_TxType:\"$TxType\"},pipeline:[{$match:{$expr:{$and:[{$eq:[\"$InstitutionID\",\"$$g_InstitutionID\"]},{$eq:[\"$TxSn\",\"$$g_TxSn\"]},{$eq:[\"$TxType\",\"$$g_TxType\"]}]}}}],as:\"ClearingTx\"}},{$unwind:{path:\"$ClearingTx.FeeVourcherID\",preserveNullAndEmptyArrays:true}},{$project:{_id:\"$SystemNo\",SystemNo: \"$SystemNo\",FeeVourcherID:\"$ClearingTx.FeeVourcherID\",TxSn:\"$TxSn\",TxDate:\"$TxDate\",TxType:\"$TxType\",ChannelID:\"$ChannelID\",ChannelName:\"$ChannelName\",InstitutionParentID:\"$InstitutionParentID\",InstitutionParentName:\"$InstitutionParentName\",InstitutionID:\"$InstitutionID\",InstitutionName:\"$InstitutionName\",TxAmount:\"$TxAmount\",Deduction:{$cond:{if:{$eq:[\"$SettlementAmount\",\"$InstitutionAmount-$PaymentAmount\"]},then:{$cond:{if:{$eq:[\"$PaymentAccountFee\",\"$Fee\"]},then:2,else:1}},else:{$cond:{if:{$eq:[\"$SettlementAmount\",\"$InstitutionAmount-$PaymentAmount-$Fee\"]},then:0,else:1}}}},Income:\"$PayerFee+$Fee\",SplitType:\"$SplitType\"}}]";
    }

    public void query(MongoCollection<Document> collection, List<Bson> bsonList) {
        MongoCursor<Document> dbCursor = collection.aggregate(bsonList).allowDiskUse(true).iterator();
        int count = 0;
        while (dbCursor.hasNext()) {
            Document item = dbCursor.next();
            count++;
            System.out.println(item.toJson());
        }
        System.out.println(count);
        dbCursor.close();
    }

}
