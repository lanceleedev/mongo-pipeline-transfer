[
    {
        $match : {
            交易日期:{$gte:"20181112", $lt:"20181113" }
        }
    },{
        $lookup : {
            from: "SY_BusinessTypeConfig",
            let: {
                g_PaymentType: "$支付类别",
                g_TxType: "$交易类型"
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
        $addFields: {
            dateTime: {$dateFromString: {
                    dateString: "$交易日期",
                    format: "%Y%m%d"
                }}
        }
    },{
        $group:{
            _id: {
                Quarter: {
                    $cond: { if: { $gte: [{ $month: "$dateTime" }, 10]}, then: "Q4", else: {
                            $cond: {  if: { $gte: [{ $month: "$dateTime" }, 7]}, then: "Q3", else: {
                                    $cond: { if: { $gte: [{ $month: "$dateTime" }, 4]}, then: "Q2", else: "Q1" }
                                }
                            }
                        }
                    }
                },
                ChannelName: "$渠道",
                InstitutionParentName: "$商户",
                InstitutionName: "$机构",
                BusinessType: {$ifNull: [ "$config.BusinessType", NumberInt("99") ]},
                Deduction: "$扣款标识"
            },
            TotalTxNum: {$sum:  NumberLong("1")},
            TotalTxAmount: {$sum: {$ifNull: [ "$交易金额", NumberDecimal("0") ]}},
            TotalIncome: {$sum: { $convert: { input: "$收入", to: "decimal", onError: "Error", onNull: NumberDecimal("0") } }},
            MaxIncomeStatus: {$max: "$业务应收收入是否标准"},
            ChannelFee: {$sum: { $convert: { input: "$渠道分润", to: "decimal", onError: "Error", onNull: NumberDecimal("0") } }},
            MaxChannelFeeStatus: {$max: "$渠道分润是否标准"},
            BankChannelFee: {$sum: { $convert: { input: "$通道应付", to: "decimal", onError: "Error", onNull: NumberDecimal("0") } }},
            MaxBankChannelFeeStatus: {$max: "$通道应付是否标准"}
        }
    },{
        $project: {
            _id: 0,
            季度: "$_id.Quarter",
            渠道: "$_id.ChannelName",
            商户: "$_id.InstitutionPakrentName",
            机构: "$_id.InstitutionName",
            业务类型: "$_id.BusinessType",
            交易笔数: "$TotalTxNum",
            交易金额: "$TotalTxAmount",
            业务应收收入: "$TotalIncome",
            业务应收收入是否标准: {$cond: {if: {$eq: ["$MaxIncomeStatus", NumberInt("10")]}, then: NumberInt("10"), else: {$cond: {if: {$gte: ["$MaxIncomeStatus", NumberInt("30")]}, then: NumberInt("30"), else: NumberInt("20")}}}},
            渠道分润: "$ChannelFee",
            渠道分润是否标准: {$cond: {if: {$eq: ["$MaxChannelFeeStatus", NumberInt("10")]}, then: NumberInt("10"), else: {$cond: {if: {$eq: ["$MaxChannelFeeStatus", NumberInt("30")]}, then: NumberInt("30"), else: NumberInt("20")}}}},
            通道应付: "$BankChannelFee",
            通道应付是否标准: {$cond: {if: {$eq: ["$MaxBankChannelFeeStatus", NumberInt("10")]}, then: NumberInt("10"), else: {$cond: {if: {$eq: ["$MaxBankChannelFeeStatus", NumberInt("30")]}, then: NumberInt("30"), else: NumberInt("20")}}}}
        }
    },{
        $addFields: {
            利润: {$subtract: [ {$subtract: [ "$业务应收收入", {$ifNull: [ "$渠道分润", NumberDecimal("0") ]} ]}, {$ifNull: [ "$通道应付", NumberDecimal("0") ]}]},
            利润是否可信: {$cond: {if: {$or: [{$eq: ["$业务应收收入是否标准", NumberInt("30")]}, {$eq: ["$业务应收收入是否标准", NumberInt("40")]}, {$eq: ["$渠道分润是否标准", NumberInt("30")]}, {$eq: ["$通道应付是否标准", NumberInt("30")]}]}, then: NumberInt("30"), else: {$cond: {if: {$and: [{$eq: ["$业务应收收入是否标准", NumberInt("10")]}, {$eq: ["$渠道分润是否标准", NumberInt("10")]}, {$eq: ["$通道应付是否标准", NumberInt("10")]}]}, then: NumberInt("10"), else: NumberInt("20")}}}}
        }
    },{
        $addFields: {
            通道应付是否标准: {$cond: {if: {$lt: ["$利润", NumberInt("0")]}, then: NumberInt("1"), else: NumberInt("0")}}
        }
    }
]