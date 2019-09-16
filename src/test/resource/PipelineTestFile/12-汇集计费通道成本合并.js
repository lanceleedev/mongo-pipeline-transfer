[
{	
	$match : {
		TxDate:{$gte:"20181112", $lt:"2018113" }
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
  $lookup : {
		 from: "SY_ChannelCost",   
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
		 as: "channel"
	}
},{
	$unwind :{ path:"$channel", preserveNullAndEmptyArrays: true}
},{
  $addFields: {
		 ChannelFee: NumberLong("0"),
		 ChannelFeeStatus: NumberLong("10")
	 }
},{
  $project: {
		 SystemNo: "$SystemNo",
	     FeeVourcherID: "$FeeVourcherID",
		 TxSn: "$TxSn",
		 TxDate: "$TxDate",
		 TxType: "$TxType",
		 ChannelID: "$ChannelID",
		 ChannelName: "$ChannelName",
		 InstitutionParentID: "$InstitutionParentID",
		 InstitutionParentName: "$InstitutionParentName",
		 InstitutionID: "$InstitutionID",
		 InstitutionName: "$InstitutionName",
		 CardType: "$channel.CardType",
		 PaymentType: "$channel.PaymentType",
		 TxAmount: "$TxAmount",
		 SplitType: "$SplitType",
		 Income: { $ifNull: [ "$fee.Income", "$Income" ] },
		 Deduction: "$Deduction",
		 IncomeStatus: {$cond: {if: {$gte: ["$fee.Income", 0]}, then: "$fee.IncomeStatus", else: 40}},
		 ChannelFee: "$ChannelFee",
		 ChannelFeeStatus: "$ChannelFeeStatus",
		 BankChannelFee: "$channel.BankChannelFee",
		 BankChannelFeeStatus: "$channel.BankChannelFeeStatus"
	}
},{
  $addFields: {
		 Profit: {$subtract: [ {$subtract: [ "$Income", {$ifNull: [ "$ChannelFee", 0 ]} ]}, {$ifNull: [ "$BankChannelFee", 0 ]}]},
		 ProfitStatus: {$cond: {if: {$or: [{$eq: ["$IncomeStatus", 30]}, {$eq: ["$IncomeStatus", 40]}, {$eq: ["$ChannelFeeStatus", 30]}, {$eq: ["$BankChannelFeeStatus", 30]}]}, then: 30, else: {$cond: {if: {$and: [{$eq: ["$IncomeStatus", 10]}, {$eq: ["$ChannelFeeStatus", 10]}, {$eq: ["$BankChannelFeeStatus", 10]}]}, then: 10, else: 20}}}}
	}
},{
  $addFields: {
		 ProfitInversion: {$cond: {if: {$lt: ["$Profit", 0]}, then: 1, else: 0}},
		 OperateTime: new Date()
	}
}
]
