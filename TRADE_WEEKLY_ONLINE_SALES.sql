create or replace view KSFPA.ONLINE_UGAM_PVT.TRADE_WEEKLY_ONLINE_SALES(
	YEAR,
	PERIOD,
	ACCOUNTING_WEEK_NUMBER,
	FY_PW,
	FYP_RANK,
	YEAR_FLAG,
	TOTAL_QUANTITY,
	TOTAL_SALES
) as
SELECT
      DISTINCT YEAR,
      PERIOD,
      RIGHT(PERIOD_WEEK,1) AS ACCOUNTING_WEEK_NUMBER,
      CONCAT('FY',YEAR,PERIOD,'W',RIGHT(PERIOD_WEEK,1)) AS FY_PW,
      DENSE_RANK() OVER(ORDER BY FY_PW DESC) AS FYP_RANK,
            CASE WHEN FYP_RANK BETWEEN 1 AND 52 THEN 'TY'
                 WHEN FYP_RANK BETWEEN 53 AND 104 THEN 'LY'
            END AS Year_Flag,
      --ORDER_TYPE,      
      SUM(QUANTITY) as TOTAL_QUANTITY,
      SUM(SALES) AS TOTAL_SALES
    FROM
      "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."SALES"."WEEKLY_ONLINE_SALES_ORDERTYPE_SUBSCRIBER_VW" A
    INNER JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" B
    ON A.FK_DATE_ID = B.SK_DATE_ID
     WHERE ACCOUNTING_YEAR >= 2022
          AND DATE <= CURRENT_DATE()-1
      GROUP BY 1,2,3,4 --ORDER_TYPE;
;