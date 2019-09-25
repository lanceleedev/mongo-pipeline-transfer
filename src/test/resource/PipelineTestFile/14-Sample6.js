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
                BusinessType: {$ifNull: [ "$config.BusinessType", NumberInt("99") ]},
                Deduction: "$Deduction"
            },
            TotalTxNum: {$sum:  NumberLong("1")},
            TotalTxAmount: {$sum: {$ifNull: [ "$TxAmount", NumberDecimal("0") ]}},
            TotalIncome: {$sum: "$Income"},
            MaxIncomeStatus: {$max: "$IncomeStatus"},
            ChannelFee: {$sum: {$ifNull: [ "$ChannelFee", NumberDecimal("0") ]}},
            MaxChannelFeeStatus: {$max: "$ChannelFeeStatus"},
            BankChannelFee: {$sum: {$ifNull: [ "$BankChannelFee", NumberDecimal("0") ]}},
            MaxBankChannelFeeStatus: {$max: "$BankChannelFeeStatus"}
        }
    },{
        $project: {
            _id: 0,
            TxDate: "$_id.TxDate",
            ChannelName: "$_id.ChannelName",
            InstitutionParentName: "$_id.InstitutionPakrentName",
            InstitutionName: "$_id.InstitutionName",
            BusinessType: "$_id.BusinessType",
            TotalTxNum: "$TotalTxNum",
            TotalTxAmount: "$TotalTxAmount",
            TotalIncome: "$TotalIncome",
            IncomeStatus: {$cond: {if: {$eq: ["$MaxIncomeStatus", NumberInt("10")]}, then: NumberInt("10"), else: {$cond: {if: {$gte: ["$MaxIncomeStatus", NumberInt("30")]}, then: NumberInt("30"), else: NumberInt("20")}}}},
            ChannelFee: "$ChannelFee",
            ChannelFeeStatus: {$cond: {if: {$eq: ["$MaxChannelFeeStatus", NumberInt("10")]}, then: NumberInt("10"), else: {$cond: {if: {$eq: ["$MaxChannelFeeStatus", NumberInt("30")]}, then: NumberInt("30"), else: NumberInt("20")}}}},
            BankChannelFee: "$BankChannelFee",
            BankChannelFeeStatus: {$cond: {if: {$eq: ["$MaxBankChannelFeeStatus", NumberInt("10")]}, then: NumberInt("10"), else: {$cond: {if: {$eq: ["$MaxBankChannelFeeStatus", NumberInt("30")]}, then: NumberInt("30"), else: NumberInt("20")}}}}
        }
    },{
        $addFields: {
            Profit: {$subtract: [ {$subtract: [ "$TotalIncome", {$ifNull: [ "$ChannelFee", NumberDecimal("0") ]} ]}, {$ifNull: [ "$BankChannelFee", NumberDecimal("0") ]}]},
            ProfitStatus: {$cond: {if: {$or: [{$eq: ["$IncomeStatus", NumberInt("30")]}, {$eq: ["$IncomeStatus", NumberInt("40")]}, {$eq: ["$ChannelFeeStatus", NumberInt("30")]}, {$eq: ["$BankChannelFeeStatus", NumberInt("30")]}]}, then: NumberInt("30"), else: {$cond: {if: {$and: [{$eq: ["$IncomeStatus", NumberInt("10")]}, {$eq: ["$ChannelFeeStatus", NumberInt("10")]}, {$eq: ["$BankChannelFeeStatus", NumberInt("10")]}]}, then: NumberInt("10"), else: NumberInt("20")}}}}
        }
    },{
        $addFields: {
            ProfitInversion: {$cond: {if: {$lt: ["$Profit", NumberInt("0")]}, then: NumberInt("1"), else: NumberInt("0")}}
        }
    }
]