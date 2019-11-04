[
    {
        $match : {
            BankTxTime:{$gte:"20190424000000", $lt:"20190425000000" }
        }
    },{
    $lookup : {
        from: "SY_FeeInfo",
        localField: "FeeVourcherID",
        foreignField: "FeeVourcherID",
        as: "fee"
    }
},{
    $unwind :{ path:"$fee", preserveNullAndEmptyArrays: true}
},{
    $project: {
        SystemNo: "$SystemNo",
        InstitutionID: "$InstitutionID",
        TxNo: "$TxNo",
        TxType: "$TxType",
        Income: { $ifNull: [ "$fee.Income", {$add : ["$PayerFee","$Fee"]} ] },
        IncomeStatus: "$fee.IncomeStatus",
        IncomeStatus: {$cond: {if: {$gte: ["$fee.Income", NumberDecimal("0")]}, then: "$fee.IncomeStatus", else: NumberInt("30")}},
        Deduction: "$Deduction",
        FeeVourcherID: "$FeeVourcherID",
        TxDate: { $substr: [ "$BankTxTime", 0, 8 ] }
    }
},{
    $addFields: {
        TxAmount: NumberDecimal("0"),
        SplitType: NumberInt("10")
    }
},{
    $lookup : {
        from: "SY_TxDetail",
        let: {
            g_TxType: "$TxType",
            g_TxNo: "$TxNo"
        },
        pipeline: [
            {
                $match:{
                    $expr:{
                        $and:
                            [
                                {$eq: ["$交易类型", "$$g_TxType"]},
                                {$eq: ["$支付平台内部流水号", "$$g_TxNo"]},
                                {$ne: ["$分账模式", 10]}
                            ]
                    }
                }
            }
        ],
        as: "tx"
    }
},{
    $unwind :{ path:"$tx", preserveNullAndEmptyArrays: true}
},{
    $project: {
        计费凭证系统流水号: "$FeeVourcherID",
        交易流水号: "$tx.交易流水号",
        支付平台内部流水号: "$TxNo",
        交易日期: "$TxDate",
        交易类型: "$tx.交易类型",
        渠道ID: "$tx.渠道ID",
        渠道: "$tx.渠道",
        商户ID: "$tx.商户ID",
        商户: "$tx.商户",
        机构ID: "$tx.机构ID",
        机构: "$tx.机构",
        卡类型: "$tx.卡类型",
        支付类别: "$tx.支付类别",
        分账模式: "$SplitType",
        交易金额: "$TxAmount",
        收入: "$Income",
        扣款标识: "$Deduction",
        业务应收收入是否标准: "$IncomeStatus"
    }
},{
    $addFields: {
        渠道分润: NumberDecimal("0"),
        渠道分润是否标准: NumberInt("10"),
        通道应付: NumberDecimal("0"),
        通道应付是否标准: NumberInt("10"),
        利润: {$subtract: [ {$subtract: [ "$收入", {$ifNull: [ "$渠道分润", NumberDecimal("0") ]} ]}, {$ifNull: [ "$通道应付", NumberDecimal("0") ]}]},
        利润是否可信: {$cond: {if: {$or: [{$eq: ["$业务应收收入是否标准", NumberInt("30")]}, {$eq: ["$业务应收收入是否标准", NumberInt("40")]}, {$eq: ["$渠道分润是否标准", NumberInt("30")]}, {$eq: ["$通道应付是否标准", NumberInt("30")]}]}, then: NumberInt("30"), else: {$cond: {if: {$and: [{$eq: ["$业务应收收入是否标准", NumberInt("10")]}, {$eq: ["$渠道分润是否标准", NumberInt("10")]}, {$eq: ["$通道应付是否标准", NumberInt("10")]}]}, then: NumberInt("10"), else: NumberInt("20")}}}}
    }
},{
    $addFields: {
        是否倒挂: {$cond: {if: {$lt: ["$Profit", NumberDecimal("0")]}, then: NumberInt("1"), else: NumberInt("0")}},
        时间戳: new Date()
    }
}
]