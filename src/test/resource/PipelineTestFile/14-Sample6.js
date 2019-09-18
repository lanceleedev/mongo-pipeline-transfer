[
    {
        $match : {
            TxDate:{$gte:"20181112", $lt:"20181113" }
        }
    },{
    $lookup : {
        from: "SY_BusinessTypeConfig",
        let: {
            g_PaymentType: "$PaymentType",
            g_TxType: "$TxType"
        },
        pipeline: [
            {
                $match:{
                    $expr:{
                        $and:
                            [
                                {$eq: ["$PaymentType", "$$g_PaymentType"]},
                                {$eq: ["$TxType", "$$g_TxType"]}
                            ]
                    }
                }
            }
        ],
        as: "config"
    }
},{
    $unwind :{ path:"$config", preserveNullAndEmptyArrays: true}
},{
    $group:{
        _id: {
            TxDate: "$TxDate",
            ChannelName: "$ChannelName",
            InstitutionParentName: "$InstitutionParentName",
            InstitutionName: "$InstitutionName",
            BussinessType: {$ifNull: [ "$config.BussinessType", NumberLong("99") ]},
            Deduction: "$Deduction"
        },
        TotalTxNum: {$sum:  NumberLong("1")},
        TotalTxAmount: {$sum: {$ifNull: [ "$TxAmount", 0 ]}},
        TotalIncome: {$sum: "$Income"},
        MaxIncomeStatus: {$max: "$IncomeStatus"},
        ChannelFee: {$sum: {$ifNull: [ "$ChannelFee", 0 ]}},
        MaxChannelFeeStatus: {$max: "$ChannelFeeStatus"},
        BankChannelFee: {$sum: {$ifNull: [ "$BankChannelFee", 0 ]}},
        MaxBankChannelFeeStatus: {$max: "$BankChannelFeeStatus"}
    }
},{
    $project: {
        TxDate: "$_id.TxDate",
        ChannelName: "$_id.ChannelName",
        InstitutionParentName: "$_id.InstitutionPakrentName",
        InstitutionName: "$_id.InstitutionName",
        BussinessType: "$_id.BussinessType",
        TotalTxNum: "$TotalTxNum",
        TotalTxAmount: "$TotalTxAmount",
        TotalIncome: "$TotalIncome",
        IncomeStatus: {$cond: {if: {$eq: ["$MaxIncomeStatus", NumberLong("10")]}, then: 10, else: {$cond: {if: {$gte: ["$MaxIncomeStatus", NumberLong("30")]}, then: NumberLong("30"), else: NumberLong("20")}}}},
        ChannelFee: "$ChannelFee",
        ChannelFeeStatus: {$cond: {if: {$eq: ["$MaxChannelFeeStatus", NumberLong("10")]}, then: 10, else: {$cond: {if: {$eq: ["$MaxChannelFeeStatus", NumberLong("30")]}, then: NumberLong("30"), else: NumberLong("20")}}}},
        BankChannelFee: "$BankChannelFee",
        BankChannelFeeStatus: {$cond: {if: {$eq: ["$MaxBankChannelFeeStatus", NumberLong("10")]}, then: 10, else: {$cond: {if: {$eq: ["$MaxBankChannelFeeStatus", NumberLong("30")]}, then: NumberLong("30"), else: NumberLong("20")}}}}
    }
},{
    $addFields: {
        Profit: {$subtract: [ {$subtract: [ "$TotalIncome", {$ifNull: [ "$ChannelFee", NumberLong("0") ]} ]}, {$ifNull: [ "$BankChannelFee", NumberLong("0") ]}]},
        ProfitStatus: {$cond: {if: {$or: [{$eq: ["$IncomeStatus", NumberLong("30")]}, {$eq: ["$IncomeStatus", NumberLong("40")]}, {$eq: ["$ChannelFeeStatus", NumberLong("30")]}, {$eq: ["$BankChannelFeeStatus", NumberLong("30")]}]}, then: NumberLong("30"), else: {$cond: {if: {$and: [{$eq: ["$IncomeStatus", NumberLong("10")]}, {$eq: ["$ChannelFeeStatus", NumberLong("10")]}, {$eq: ["$BankChannelFeeStatus", NumberLong("10")]}]}, then: NumberLong("10"), else: NumberLong("20")}}}}
    }
},{
    $addFields: {
        ProfitInversion: {$cond: {if: {$lt: ["$Profit", NumberLong("0")]}, then: NumberLong("1"), else: NumberLong("0")}}
    }
}
]