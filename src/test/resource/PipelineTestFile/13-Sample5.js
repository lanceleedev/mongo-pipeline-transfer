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
        TxSn: "$TxSn",
        TxType: "$TxType",
        Income: "$fee.Income",
        IncomeStatus: "$fee.IncomeStatus",
        Deduction: "$Deduction",
        FeeVourcherID: "$FeeVourcherID",
        TxDate: { $substr: [ "$BankTxTime", 0, 8 ] }
    }
},{
    $addFields: {
        TxAmount: NumberLong("0"),
        SplitType: NumberLong("10")
    }
},{
    $lookup : {
        from: "SY_TxDetail",
        let: {
            g_InstitutionID: "$InstitutionID",
            g_TxSn: "$TxSn",
            g_TxType: "$TxType"
        },
        pipeline: [
            {
                $match:{
                    $expr:{
                        $and:
                            [
                                {$eq: ["$InstitutionID", "$$g_InstitutionID"]},
                                {$eq: ["$TxSn", "$$g_TxSn"]},
                                {$eq: ["$TxType", "$$g_TxType"]}
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
        SystemNo: "$SystemNo",
        FeeVourcherID: "$FeeVourcherID",
        TxSn: "$tx.TxSn",
        TxDate: "$TxDate",
        TxType: "$tx.TxType",
        ChannelID: "$tx.ChannelID",
        ChannelName: "$tx.ChannelName",
        InstitutionParentID: "$tx.InstitutionParentID",
        InstitutionParentName: "$tx.InstitutionParentName",
        InstitutionID: "$tx.InstitutionID",
        InstitutionName: "$tx.InstitutionName",
        CardType: "$tx.CardType",
        PaymentType: "$tx.PaymentType",
        Income: "$Income",
        Deduction: "$Deduction",
        IncomeStatus: "$IncomeStatus"
    }
},{
    $addFields: {
        ChannelFee: NumberLong("0"),
        ChannelFeeStatus: NumberLong("10"),
        BankChannelFee: NumberLong("0"),
        BankChannelFeeStatus: NumberLong("10"),
        Profit: {$subtract: [ {$subtract: [ "$Income", {$ifNull: [ "$ChannelFee", NumberLong("0") ]} ]}, {$ifNull: [ "$BankChannelFee", NumberLong("0") ]}]},
        ProfitStatus: {$cond: {if: {$or: [{$eq: ["$IncomeStatus", NumberLong("30")]}, {$eq: ["$IncomeStatus", NumberLong("40")]}, {$eq: ["$ChannelFeeStatus", NumberLong("30")]}, {$eq: ["$BankChannelFeeStatus", NumberLong("30")]}]}, then: NumberLong("30"), else: {$cond: {if: {$and: [{$eq: ["$IncomeStatus", NumberLong("10")]}, {$eq: ["$ChannelFeeStatus", NumberLong("10")]}, {$eq: ["$BankChannelFeeStatus", NumberLong("10")]}]}, then: NumberLong("10"), else: NumberLong("20")}}}}
    }
},{
    $addFields: {
        ProfitInversion: {$cond: {if: {$lt: ["$Profit", NumberLong("0")]}, then: NumberLong("1"), else: NumberLong("0")}},
        OperateTime: new Date()
    }
}
]