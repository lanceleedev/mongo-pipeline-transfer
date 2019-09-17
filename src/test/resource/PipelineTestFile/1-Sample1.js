[
    {
        $match :{
            BankResponseTime:{$gte:"20171017152400",$lt:"20171017164640" },
            Status:{$gte:20, $lte:30}
        }
    },
    {
        $group:{
            _id: {
                日期过度: {$substr: ["$BankResponseTime", 0, 8]},
                InstitutionID: "$InstitutionID",
                SourceTxType: "$SourceTxType",
                AccountType: "$AccountType",
                IssueBankID: "$IssueBankID",
                CardType: "$CardType",
                FinanceInstCode: "$FinanceInstCode",
                GatheringWay: "$GatheringWay",
                ResponseCode: "$ResponseCode",
                ResponseMessage: "$ResponseMessage",
                BankResponseCode: "$BankResponseCode",
                BankResponseMessage: "$BankResponseMessage",
                Status: "$Status",
                MerchantID: "$MerchantID",
                MerchantName: "$MerchantName"
            },
            交易金额: {$sum: "$Amount"},
            交易笔数: {$sum: 1}
        }
    }
    ,
    {
        $lookup:{
            from: "JC_TypeInfo",
            let: { g_txType: "$_id.SourceTxType" },
            pipeline: [
                {
                    $match:{
                        $expr:{
                            $and:
                                [
                                    {$eq: ["$BusinessDescription", "代收"]},
                                    {$eq: ["$TxType", "$$g_txType"]}
                                ]
                        }
                    }
                }
            ],
            as: "resultdoc"
        }
    }
    ,
    {
        $lookup:{
            from: "JC_Institution",
            let: { g_InstitutionID: "$_id.InstitutionID" },
            pipeline: [
                {
                    $match:{
                        $expr:{
                            $and:
                                [
                                    {$eq: ["$InstitutionID", "$$g_InstitutionID"]}
                                ]
                        }
                    }
                }
            ],
            as: "resultdoc2"
        }
    }
    ,
    {
        $lookup:{
            from: "JC_BankInfo",
            let: { g_IssueBankID: "$_id.IssueBankID" },
            pipeline: [
                {
                    $match:{
                        $expr:{
                            $and:
                                [
                                    {$eq: ["$BankID", "$$g_IssueBankID"]}
                                ]
                        }
                    }
                }
            ],
            as: "resultdoc4"
        }
    }
    ,
    {
        $lookup:{
            from: "JC_BankInfo",
            let: { g_FinanceInstCode: "$_id.FinanceInstCode" },
            pipeline: [
                {
                    $match:{
                        $expr:{
                            $and:
                                [
                                    {$eq: ["$BankID", "$$g_FinanceInstCode"]}
                                ]
                        }
                    }
                }
            ],
            as: "resultdoc5"
        }
    }
    ,
    {
        $group:{
            _id: {
                状态: {$cond: {if: {$eq: ["$_id.Status", 20]}, then: "成功", else: "失败"}},
                日期过度: "$_id.日期过度",

                交易类型代码: "$_id.SourceTxType",
                机构ID: "$_id.InstitutionID",
                业务类型: "$resultdoc.BusinessDescription",
                产品分类: "$resultdoc.ProductCategory",
                交易类型说明: "$resultdoc.TxTypeDescription",
                实际业务类型: {$cond: {if: {$eq: ["$_id.GatheringWay", "11"]}, then: "单笔代收", else: {$cond: {if: {$eq: ["$_id.GatheringWay","12"]}, then: "批量代收", else: ""}}}},
                银行ID: "$_id.IssueBankID",
                银行名称: "$resultdoc4.BankName",
                通道ID: "$_id.FinanceInstCode",
                通道名称: "$resultdoc5.BankName",
                卡类型: {$cond: {if: {$eq: ["$_id.CardType", "10"]}, then: "借记卡", else: {$cond: {if: {$eq: ["$_id.CardType","20"]}, then: "贷记卡", else: {$cond: {if: {$eq: ["$_id.CardType","30"]}, then: "存折",
                                        else: {$cond: {if: {$eq: ["$_id.CardType","40"]}, then: "电子账户" ,else: ""}}}}}}}},
                账户类型: {$cond: {if: {$eq: ["$_id.AccountType", 11]}, then: "个人", else: {$cond: {if: {$eq: ["$_id.AccountType",12]}, then: "企业", else: ""}}}},

                中金响应代码: "$_id.ResponseCode",
                中金响应消息: "$_id.ResponseMessage",
                银行响应代码: "$_id.BankResponseCode",
                银行响应消息: "$_id.BankResponseMessage",
                二级商户号: "$_id.MerchantID",
                二级商户名称: "$_id.MerchantName",

                机构名称: "$resultdoc2.InstitutionName",
                机构简称: "$resultdoc2.InstitutionShortName",
                一级机构ID: "$resultdoc2.InstitutionParentID",
                一级机构名称: "$resultdoc2.InstitutionParentName",
                业务经理: "$resultdoc2.MarketingManager",
                机构创建时间:  { $ifNull: [ "$resultdoc2.CreateTime", "20150101" ] },
                机构复核时间: { $ifNull: [ "$resultdoc2.CheckTime", "20150101" ] },
                所属区域: { $ifNull: [ "$resultdoc2.Area", "" ] },
                所属行业: { $ifNull: [ "$resultdoc2.Trade", "" ] },
                商服人员: { $ifNull: [ "$resultdoc2.BusinessServices", "" ] },
                技术支持: { $ifNull: [ "$resultdoc2.CPCNSupports", "" ] },
                商户所属: { $ifNull: [ "$resultdoc2.BusinessArea", "" ] }
            },
            实际交易金额分: {$sum: {$cond: {if: {$eq: ["$_id.Status", 20]}, then: "$交易金额", else: 0}}},
            实际交易笔数: {$sum: {$cond: {if: {$eq: ["$_id.Status", 20]}, then: "$交易笔数", else: 0}}},

            失败交易金额分: {$sum: {$cond: {if: {$eq: ["$_id.Status", 30]}, then: "$交易金额", else: 0}}},
            失败交易笔数: {$sum: {$cond: {if: {$eq: ["$_id.Status", 30]}, then: "$交易笔数", else: 0}}},

            总交易金额分: {$sum: "$交易金额"},
            总交易笔数: {$sum: "$交易笔数"}
        }
    }
    ,{$unwind :{ path:"$_id.交易类型代码", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.业务类型", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.产品分类", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.交易类型说明", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.业务类型", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.银行名称", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.通道名称", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.业务类型", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.机构名称", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.机构简称", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.一级机构ID", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.一级机构名称", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.业务经理", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.机构创建时间", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.机构复核时间", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.所属区域", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.所属行业", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.商服人员", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.技术支持", preserveNullAndEmptyArrays: true}}
    ,{$unwind :{ path:"$_id.商户所属", preserveNullAndEmptyArrays: true}}
    ,{
    $project:
        {
            _id:0,
            状态: { $ifNull: [ "$_id.状态", "" ] },
            日期过度: { $ifNull: [ "$_id.日期过度", "" ] },
            交易类型代码: { $ifNull: [ "$_id.交易类型代码", "" ] },
            机构ID: { $ifNull: [ "$_id.机构ID", "" ] },
            业务类型: { $ifNull: [ "$_id.业务类型", "" ] },
            产品分类: { $ifNull: [ "$_id.产品分类", "" ] },
            交易类型说明: { $ifNull: [ "$_id.交易类型说明", "" ] },
            实际业务类型: { $ifNull: [ "$_id.实际业务类型", "" ] },
            银行ID: { $ifNull: [ "$_id.银行ID", "" ] },
            银行名称: { $ifNull: ["$_id.银行名称", "" ] },
            通道ID: { $ifNull: [ "$_id.通道ID", "" ] },
            通道名称: { $ifNull: [ "$_id.通道名称", "" ] },
            卡类型: { $ifNull: [ "$_id.卡类型", "" ] },
            账户类型: { $ifNull: [ "$_id.账户类型", "" ] },
            中金响应代码: { $ifNull: [ "$_id.中金响应代码", "" ] },
            中金响应消息: { $ifNull: [ "$_id.中金响应消息", "" ] },
            银行响应代码:{ $ifNull: [ "$_id.银行响应代码", "" ] },
            银行响应消息: { $ifNull: [ "$_id.银行响应消息", "" ] },
            二级商户号: { $ifNull: [ "$_id.二级商户号", "" ] },
            二级商户名称:{ $ifNull: [ "$_id.二级商户名称", "" ] },
            机构名称: { $ifNull: [ "$_id.机构名称", "" ] },
            机构简称: { $ifNull: [ "$_id.机构简称",  "" ] },
            一级机构ID: { $ifNull: [ "$_id.一级机构ID", "" ] },
            一级机构名称: { $ifNull: [ "$_id.一级机构名称", "" ] },
            业务经理: { $ifNull: [ "$_id.业务经理", "" ] },
            机构创建时间:   {$substr: [ { $ifNull: [ "$_id.机构创建时间", "20150101" ] } , 0, 8]} ,
            机构复核时间:   {$substr: [ { $ifNull: [ "$_id.机构复核时间", "20150101" ] } , 0, 8]} ,
            所属区域: { $ifNull: [ "$_id.所属区域", "" ] },
            所属行业: { $ifNull: [ "$_id.所属行业", "" ] },
            商服人员: { $ifNull: [ "$_id.商服人员", "" ] },
            技术支持: { $ifNull: [ "$_id.技术支持", "" ] },
            商户所属: { $ifNull: [ "$_id.商户所属", "" ] },
            实际交易金额分: {
                "$multiply" : [
                    "$实际交易金额分",
                    { "$numberLong": "1" }
                ]
            },
            实际交易笔数: 1,
            失败交易金额分: {
                "$multiply" : [
                    "$失败交易金额分",
                    { "$numberLong": "1" }
                ]
            },
            失败交易笔数: 1,
            总交易金额分: {
                "$multiply" : [
                    "$总交易金额分",
                    { "$numberLong": "1" }
                ]
            },
            总交易笔数: 1
        }
}
]