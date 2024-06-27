create or replace view KSFPA.ONLINE_UGAM_PVT.TRADE_WEEKLY_TXN_SHIPMENT(
	ACCOUNTING_YEAR,
	PERIOD,
	ACCOUNTING_WEEK_NUMBER,
	FY_PW,
	FYP_RANK,
	YEAR_FLAG,
	ORDER_TYPE,
	TOTAL_TRANS,
	SHIPMENT_COUNT,
	TOTAL_QUANT
) as 
SELECT
      ACCOUNTING_YEAR,
            CONCAT('P',RIGHT(ACCOUNTING_PERIOD_NUMBER,2)) AS PERIOD,
            ACCOUNTING_WEEK_NUMBER,
            CONCAT('FY',ACCOUNTING_YEAR,'P',RIGHT(ACCOUNTING_PERIOD_NUMBER,2),'W',ACCOUNTING_WEEK_NUMBER) AS FY_PW,
            DENSE_RANK() OVER(ORDER BY FY_PW DESC) AS FYP_RANK,
            CASE WHEN FYP_RANK BETWEEN 1 AND 52 THEN 'TY'
                 WHEN FYP_RANK BETWEEN 53 AND 104 THEN 'LY'
            END AS Year_Flag,
      ORDER_TYPE,
      SUM(TRANSACTION_COUNT) AS TOTAL_TRANS,
      SUM(SHIPMENT_COUNT) AS SHIPMENT_COUNT,
      SUM(QUANTITY) AS TOTAL_QUANT
    FROM
      "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."SALES"."WEEKLY_ONLINE_TXN_SHIPMENT_ORDERTYPE_SUBSCRIBER_VW" A
    INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" B
    ON A.FK_WEEK_ID = B.SK_DATE_ID
    WHERE ACCOUNTING_YEAR >= 2022
         AND DATE <= CURRENT_DATE()-1
    GROUP BY 1,2,3,4,7;