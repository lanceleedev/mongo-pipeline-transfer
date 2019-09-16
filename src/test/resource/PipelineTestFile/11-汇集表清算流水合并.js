[
{	
	$match : {
		TxDate:{$gte:"20181112", $lt:"2018113" }
	}
},
{
  $lookup : {
		 from: "SY_ClearingTxTmp",   
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
		 as: "ClearingTx"
	}
},{
	$unwind :{ path:"$ClearingTx", preserveNullAndEmptyArrays: true}
},{
  $project: {
		 SystemNo: "$SystemNo",
	   	 FeeVourcherID:{ $ifNull: [ "$ClearingTx.FeeVourcherID", "" ] },
		 TxSn: "$TxSn",
		 TxDate: "$TxDate",
		 TxType: "$TxType",
		 ChannelID: "$ChannelID",
		 ChannelName: "$ChannelName",
		 InstitutionParentID: "$InstitutionParentID",
		 InstitutionParentName: "$InstitutionParentName",
		 InstitutionID: "$InstitutionID",
		 InstitutionName: "$InstitutionName",
		 TxAmount: "$TxAmount",
		 Deduction: {$cond: {if: {$eq: ["$SettlementAmount", "$InstitutionAmount - $PaymentAmount"]}, then: {$cond: {if: {$eq: ["$PaymentAccountFee", "$Fee"]}, then: 2, else: 1}}, else: {$cond: {if: {$eq: ["$SettlementAmount", "$InstitutionAmount - $PaymentAmount - $Fee"]}, then: 0, else: 1}}}},
		 Income: {$add : ["$PayerFee","$Fee"]},
		 SplitType: "$SplitType"
	}
}
]