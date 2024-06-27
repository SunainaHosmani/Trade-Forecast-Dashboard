create or replace view KSFPA.ONLINE_UGAM_PVT.CC_SLA_WITHOUT_DLS(
	FK_DATE,
	FY_PW,
	STORE_ID,
	STATE,
	ZONE,
	STORE_STATUS,
	SHIPMENTS,
	TOTALSHIPMENTS
) as

SELECT 
       DATE(DO_Created) AS FK_DATE,
       FY_PW,
       STORE_ID,
       STATE,
       ZONE,
       STORE_STATUS,
       SHIPMENTS,
       TotalShipments
FROM (
    SELECT STORE_ID,
           STATE,
           ZONE,
           STORE_STATUS,
           FY_PW,
           DO_Created,
           SUM(CASE WHEN DATE(LOCAL_DO_Created) = DATE(LOCAL_Collected_DATE)
                         AND LOCAL_Collected_Hr <= 16 THEN TotalShipment END) AS SHIPMENTS,
           SUM(TotalShipment) AS TotalShipments
    FROM (
        SELECT STORE_ID,
               STATE,
               ZONE,
               STORE_STATUS,
               DO_Created,
               concat ('FY', DD.ACCOUNTING_YEAR,
                                'P',
                                RIGHT (DD.ACCOUNTING_PERIOD_DESCRIPTION,2),
                                'W',
                                DD.ACCOUNTING_WEEK_NUMBER
                            ) AS FY_PW,
               ReadytoCollectDate,
               ReadyToCollect_hr,
            CASE --WHEN LTRIM(RTRIM(STATE)) ='QLD' THEN DATEADD(HOUR,-1,ReadytoCollectDate) 
            WHEN LTRIM(RTRIM(STATE)) ='NTH' THEN DATEADD(HOUR,2,ReadytoCollectDate) 
            WHEN LTRIM(RTRIM(STATE)) ='STH' THEN DATEADD(HOUR,2,ReadytoCollectDate) 
            WHEN LTRIM(RTRIM(STATE)) ='S.A' THEN DATEADD(HOUR,-0.5,ReadytoCollectDate)
            WHEN LTRIM(RTRIM(STATE)) ='W.A' THEN DATEADD(HOUR,-2,ReadytoCollectDate)
            --WHEN LTRIM(RTRIM(STATE)) ='TAS' THEN DATEADD(HOUR,-1.5,ReadytoCollectDate)
            WHEN LTRIM(RTRIM(STATE)) ='N.T' THEN DATEADD(HOUR,-0.5,ReadytoCollectDate)
            ELSE ReadytoCollectDate END AS LOCAL_ReadytoCollectDate,
            CASE --WHEN LTRIM(RTRIM(STATE)) ='QLD' THEN DATEADD(HOUR,-1,DO_Created) 
            WHEN LTRIM(RTRIM(STATE)) ='NTH' THEN DATEADD(HOUR,2,DO_Created) 
            WHEN LTRIM(RTRIM(STATE)) ='STH' THEN DATEADD(HOUR,2,DO_Created) 
            WHEN LTRIM(RTRIM(STATE)) ='S.A' THEN DATEADD(HOUR,-0.5,DO_Created)
            WHEN LTRIM(RTRIM(STATE)) ='W.A' THEN DATEADD(HOUR,2,DO_Created)
            --WHEN LTRIM(RTRIM(STATE)) ='TAS' THEN DATEADD(HOUR,-1.5,DO_Created)
            WHEN LTRIM(RTRIM(STATE)) ='N.T' THEN DATEADD(HOUR,-0.5,DO_Created)
            ELSE DO_Created END AS LOCAL_DO_Created,
            HOUR(LOCAL_DO_Created) as LOCAL_DO_Created_Hr,
            CASE --WHEN LTRIM(RTRIM(STATE)) ='QLD' THEN DATEADD(HOUR,-1,Collected_DATE) 
            WHEN LTRIM(RTRIM(STATE)) ='NTH' THEN DATEADD(HOUR,2,Collected_DATE) 
            WHEN LTRIM(RTRIM(STATE)) ='STH' THEN DATEADD(HOUR,2,Collected_DATE) 
            WHEN LTRIM(RTRIM(STATE)) ='S.A' THEN DATEADD(HOUR,-0.5,Collected_DATE)
            WHEN LTRIM(RTRIM(STATE)) ='W.A' THEN DATEADD(HOUR,-2,Collected_DATE)
            --WHEN LTRIM(RTRIM(STATE)) ='TAS' THEN DATEADD(HOUR,-1.5,Collected_DATE)
            WHEN LTRIM(RTRIM(STATE)) ='N.T' THEN DATEADD(HOUR,-0.5,Collected_DATE)
            ELSE Collected_DATE END AS LOCAL_Collected_DATE,
            HOUR(LOCAL_Collected_DATE) as LOCAL_Collected_Hr,
            COUNT(DISTINCT DD_ORDERID) AS TotalShipment 
        FROM (
           SELECT 
				so.OrderID as DD_orderID
		,       LOCATION_CODE AS Store_ID
        ,       fs.State as State 
		,		ZONE		
		,		STORE_STATUS
        ,       so.OrderStatus as ORDER_STATUS
        ,       so.DO_Created AS  DO_Created
        ,       HOUR(so.DO_Created) AS DO_Created_Hr
        ,       so.DO_ReadyToCollect AS ReadytoCollectDate
        ,       HOUR(so.DO_ReadyToCollect) AS ReadyToCollect_hr
        --,     DATE(DO_LOCATION) AS Last_Carton_Put_Away
        --,     HOUR(sc.DO_LOCATION) AS Last_Carton_Put_Away_hr
        --,(CASE WHEN sc.DO_LOCATION is Null then DO_ReadyToCollect else DO_LOCATION END) AS Collected_DATE 
        --,(CASE WHEN sc.DO_LOCATION is Null then HOUR(DO_ReadyToCollect) else HOUR(DO_LOCATION) END) AS Collected_HR
          ,(CASE WHEN DO_LOC IS NULL OR DO_ReadyToCollect < DO_LOC THEN DO_ReadyToCollect 
            ELSE DO_LOC END) AS Collected_DATE,
           (CASE WHEN DO_LOC IS NULL OR HOUR(DO_ReadyToCollect) < HOUR(DO_LOC) THEN HOUR(DO_ReadyToCollect)
            ELSE HOUR(DO_LOC) END) AS Collected_HR
from
(
select * 
from   "KSFPA"."OMS"."STOREORDER_REALTIME"
where           
        (CARRIER NOT IN ('STORE2STORE','undefined') OR CARRIER is null)
        AND(CARRIER NOT IN ('CCDCST') OR CARRIER is null)
) so
  Left join 
  ( SELECT OrderID, MAX(DO_LOCATION) as DO_LOC
    FROM "KSFPA"."OMS"."SHIPMENTCARTON" 
    GROUP BY OrderID ) sc
  ON so.OrderID=sc.OrderID
left join       "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_LOCATION" fs 
on              so.Str_ID = fs.LOCATION_CODE
left join       "KSFPA"."ONLINE_UGAM_PVT"."ORCH_STORES" STR
on              fs.LOCATION_CODE = STR.STORE
where           rtrim(ltrim(so.OrderStatus)) not in ('OrderCancelled', 'OrderReferToCSC','OrderInTransit','PartiallyInTransit','undefined') 
and             DATE(so.DO_Created) >= Last_day(date('2021-06-28') -7,'week')+1
and             DATE(so.DO_Created) <= Last_day(date(current_date()),'week') 
--and             DATE(so.DO_Created) >= Last_day(date($start_dt) -7,'week')+1
--and             DATE(so.DO_Created) <= Last_day(date($end_dt),'week') 
and             HOUR(so.DO_Created) BETWEEN 0 AND 15 
and             so.str_ID not in ('1000','9999','9998')
and             so.Instore_Flag = 1 -- C & C 
and             LOCATION_CODE <> 'undefined'
and             LOCATION_NAME <> 'undefined'

        ) t1
        --WHERE LOCAL_DO_Created BETWEEN $START_DT AND $END_DT 
        LEFT JOIN 
        "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" dd
        ON TO_DATE(DO_Created) = dd.Date  
        AND LOCAL_DO_Created_Hr BETWEEN 0 AND 11     
        AND ACCOUNTING_YEAR >= 2021
        GROUP BY 
            STORE_ID,
            STATE,
            ZONE,
            STORE_STATUS,
            FY_PW,
            DO_Created,
            ReadytoCollectDate,
            ReadyToCollect_hr,
            LOCAL_ReadytoCollectDate,
            LOCAL_DO_Created,
            LOCAL_DO_Created_Hr,
            LOCAL_Collected_DATE,
            LOCAL_Collected_Hr,
            DATE(LOCAL_DO_CREATED)
    )
    GROUP BY 
        STORE_ID,
        STATE,
        ZONE,
        STORE_STATUS,
        FY_PW,
        DO_Created
) t2;