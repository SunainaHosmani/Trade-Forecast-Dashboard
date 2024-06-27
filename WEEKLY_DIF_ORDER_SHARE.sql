create or replace view KSFPA.ONLINE_UGAM_PVT.WEEKLY_DIF_ORDER_SHARE(
	FY_PW,
	FK_CREATED_DATE,
	STATE,
	ORDER_TYPE,
	EXT_ORDERS_RECEVIED_CAC,
	EXT_ORDERS_RECEVIED_STD,
	EXT_ORDERS_RECEVIED_GRAND_TOTAL
) as
(SELECT 
  CONCAT('FY', ACCOUNTING_YEAR,'_P',ACCOUNTING_MONTH_NUMBER,'W',ACCOUNTING_WEEK_NUMBER) AS FY_PW,
  FK_CREATED_DATE,
  STATE,
  ORDER_TYPE,
 SUM(Ext_Orders_Recevied_CAC) AS Ext_Orders_Recevied_CAC,
SUM(Ext_Orders_Recevied_STD) AS Ext_Orders_Recevied_STD,
SUM(Ext_Orders_Recevied_Grand_Total) AS Ext_Orders_Recevied_Grand_Total
  FROM 
((
SELECT DISTINCT
   FK_CREATED_DATE,
  STATE,
  ORDER_TYPE,
COUNT(DISTINCT(CASE WHEN ORDER_TYPE IN ('CAC') THEN DD_EXTERNALORDERID END )) AS Ext_Orders_Recevied_CAC,
COUNT(DISTINCT(CASE WHEN ORDER_TYPE IN ('STD') THEN DD_EXTERNALORDERID END )) AS Ext_Orders_Recevied_STD,
COUNT(DISTINCT(CASE WHEN ORDER_TYPE IN ('CAC') THEN DD_EXTERNALORDERID END )) + COUNT(DISTINCT(CASE WHEN ORDER_TYPE IN ('STD') THEN DD_EXTERNALORDERID END )) AS Ext_Orders_Recevied_Grand_Total
  FROM "KSFPA"."ONLINE_UGAM_PVT"."NATHAN_CUSTOMER_UNITS_RECEIVED" N
LEFT JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."ORDER_MANAGEMENT"."FACT_ORDER_DETAIL" F
ON N.ORDERID = F.DD_ORDERID
AND N.ORDER_TYPE = F.DD_CUSTOMER_ORDER_TYPE
WHERE STATE  NOT IN ('undefined')

GROUP BY 1,2,3
) 
UNION
(
SELECT 
  FK_CREATED_DATE,
  STATE,
  'All' AS ORDER_TYPE,
  COUNT(DISTINCT(CASE WHEN ORDER_TYPE IN ('CAC') THEN DD_EXTERNALORDERID END )) AS Ext_Orders_Recevied_CAC,
COUNT(DISTINCT(CASE WHEN ORDER_TYPE IN ('STD') THEN DD_EXTERNALORDERID END )) AS Ext_Orders_Recevied_STD,
COUNT(DISTINCT DD_EXTERNALORDERID) AS Ext_Orders_Recevied_Grand_Total
  FROM "KSFPA"."ONLINE_UGAM_PVT"."NATHAN_CUSTOMER_UNITS_RECEIVED" N
  LEFT JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."ORDER_MANAGEMENT"."FACT_ORDER_DETAIL" F
ON N.ORDERID = F.DD_ORDERID
AND N.ORDER_TYPE = F.DD_CUSTOMER_ORDER_TYPE
WHERE STATE  NOT IN ('undefined')
GROUP BY 1,2,3
))A
  LEFT JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" D
on A.FK_CREATED_DATE=D.DATE
WHERE STATE  NOT IN ('undefined')
GROUP BY 1,2,3,4
);