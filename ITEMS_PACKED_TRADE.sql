create or replace view KSFPA.ONLINE_UGAM_PVT.ITEMS_PACKED_TRADE(
	STR_ID,
	STATE,
	ORDER_TYPE,
	FY_PW,
	FK_DATE,
	PACKINGTIME,
	ITEMSPACKED,
	CARTONSPACKED,
	PACKINGEFFICIENCYPERITEM,
	PACKINGEFFICIENCYPERCARTON
) as
SELECT              
                    STR_ID, 
                    State,
                    Order_type,
                    FY_PW,
                    Date(EndDate) as FK_Date,
                    SUM(PackingTime) AS PackingTime, 
                    SUM(CARTONNUMOFITEMS) AS ItemsPacked,  
                    COUNT(1) AS CartonsPacked,  
                    CASE
                           WHEN SUM(CartonNumOfItems) = 0 THEN 0
                           ELSE ROUND(SUM(PackingTime)/ SUM(CartonNumOfItems),2)  
                    END AS PackingEfficiencyPerItem,
                    CASE
                           WHEN COUNT(1) = 0 THEN 0
                           ELSE ROUND(SUM(PackingTime)/ COUNT(1),2)  
                    END AS PackingEfficiencyPerCarton
                    FROM
                    (      SELECT 
                            STR_ID,
                            LO.STATE as STATE,
                            cart.ORDERID, 
                            'All' as Order_type,
                            cart.DO_CREATED AS StartDate, 
                            DO_PACKING AS EndDate, 
                            concat ('FY', DD.ACCOUNTING_YEAR,
                                'P',
                                RIGHT (DD.ACCOUNTING_PERIOD_DESCRIPTION,2),
                                'W',
                                DD.ACCOUNTING_WEEK_NUMBER
                           ) AS FY_PW,
                           CASE
                                 WHEN (ROUND(CAST(DATEDIFF(s, cart.DO_CREATED, DO_PACKING) AS float)/ 60, 2)) <= 0 THEN  0.88
                                 WHEN (ROUND(CAST(DATEDIFF(s, cart.DO_CREATED, DO_PACKING) AS float)/ 60, 2)) < 0.5 THEN  3.7
                                 WHEN (ROUND(CAST(DATEDIFF(s, cart.DO_Created, DO_Packing) AS float)/60, 2)) >= 30  THEN 5
                                 ELSE ROUND(CAST(DATEDIFF(s, cart.DO_CREATED, DO_PACKING) AS float)/ 60, 2)
                           END AS PackingTime,  
                           CARTONNUMOFITEMS,
                           CARTONNUM 
                     FROM "KSFPA"."OMS"."SHIPMENTCARTON" cart
                     INNER JOIN "KSFPA"."OMS"."CUSTOMERORDER_REALTIME" CUST
                     ON CUST.ORDERID = cart.ORDERID
                     LEFT JOIN "KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD"."COMMON_DIMENSIONS"."DIM_DATE" DD
                     ON TO_DATE(DO_PACKING) = dd.Date 
                     JOIN KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_LOCATION LO
                     ON CAST(cart.STR_ID AS VARCHAR) = LO.LOCATION_CODE
                     WHERE (NVL(CARTONNUMOFITEMS, 0) > 0)
//                   AND (STR_ID = NVL('ALL',STR_ID)) --commented
                     AND DO_PACKING > '2021-01-01 00:00:000.000'
                     AND DO_PACKING <= current_date()
                     AND ACCOUNTING_YEAR >= 2022
                    --AND (CarrierShipmentID > 0)
                    --GROUP BY Str_ID, OrderID, CarrierShipmentID
                    ) PACKING 
         GROUP BY STR_ID,Order_type,FY_PW, State, EndDate;