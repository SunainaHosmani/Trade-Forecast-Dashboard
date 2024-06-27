create or replace view KSFPA.ONLINE_UGAM_PVT.PICK_PACK_TRADE(
	ORDER_DATE,
	FY_PW,
	STORE,
	STORE_NAME,
	STATE,
	TOTAL_PICKING_TIME_MIN,
	TOTAL_ORDERS_PICKED,
	TOTAL_ITEMS_PICKED,
	TOTAL_ITEMS_REQUIRED,
	TOTAL_PACKING_MIN,
	TOTAL_ITEMS_PACKED,
	TOTAL_CARTONS_PACKED,
	PICKING_EFFICIENCY_PER_ORDER_MIN,
	PICKING_EFFICIENCY_PER_ITEM_PICKED_MIN,
	PICKING_EFFICIENCY_PER_ITEM_REQUIRED_MIN,
	PACKING_EFFICIENCY_PER_ITEM_MIN,
	PACKING_EFFICIENCY_PER_CARTON_MIN,
	ADDITIONAL_TIME_FOR_ITEMS_PACKED_MIN,
	TOTAL_FULFILLMENT_TIME_MIN,
	HOURS_WORKED,
	ORDER_TYPE
) as

SELECT 
    --"Date" as DATE,
    DATE("Order Date") AS ORDER_DATE,
     FY_PW,
	"Store" as STORE,
    "Store Name" AS STORE_NAME,
    "State" as State,
    --"ExternalOrder ID" AS EXTERNALORDERID,
    --"Order ID" AS ORDERID,
    SUM("Total Picking Time (min)") AS TOTAL_PICKING_TIME_MIN,
    SUM("Total Orders Picked") AS TOTAL_ORDERS_PICKED,
    SUM("Total Items Picked") AS TOTAL_ITEMS_PICKED,
    SUM("Total Items Required") AS TOTAL_ITEMS_REQUIRED,
    SUM("Total Packing Time (min)")  AS TOTAL_PACKING_MIN,
    SUM("Total Items Packed") AS TOTAL_ITEMS_PACKED,
    SUM("Total Cartons Packed") AS TOTAL_CARTONS_PACKED,
    AVG("Picking Efficiency Per Order (min)") AS PICKING_EFFICIENCY_PER_ORDER_MIN,
    AVG("Picking Efficiency Per Item Picked (min)") AS PICKING_EFFICIENCY_PER_ITEM_PICKED_MIN,
    AVG("Picking Efficiency Per Item Required (min)") AS PICKING_EFFICIENCY_PER_ITEM_REQUIRED_MIN,
    AVG("Packing Efficiency Per Item (min)") AS PACKING_EFFICIENCY_PER_ITEM_MIN,
    AVG("Packing Efficiency Per Carton (min)") AS PACKING_EFFICIENCY_PER_CARTON_MIN,
    SUM("Additional Time For Items Packed (min)") AS ADDITIONAL_TIME_FOR_ITEMS_PACKED_MIN,
    SUM((NVL("Total Picking Time (min)",0) + NVL("Total Packing Time (min)",0) + NVL("Additional Time For Items Packed (min)",0))) AS TOTAL_FULFILLMENT_TIME_MIN,
    TOTAL_FULFILLMENT_TIME_MIN/60 as Hours_Worked,
    CASE WHEN RTRIM(LTRIM(CO.CUSTOMERORDERTYPE)) = 'CAC' THEN 'CC' 
    WHEN CO.CUSTOMERORDERTYPE = 'STD' THEN 'HD'
    END AS ORDER_TYPE

FROM
    (SELECT 
     		DISTINCT PIC."DATE" AS "Date",
            TO_DATE(FOD.TRANSACTION_TIMESTAMP) AS "Order Date",
     		FS.LOCATION_CODE AS "Store",
            FS.LOCATION_NAME AS "Store Name",
            FS.STATE AS "State",
            FOD.DD_EXTERNALORDERID AS "ExternalOrder ID",
     		FOD.DD_ORDERID AS "Order ID",
            FY_PW,
     		ROUND(PIC.PickingTime,2) AS "Total Picking Time (min)",
     		PIC.OrdersPicked AS "Total Orders Picked",
     		CAST(PIC.ItemsPicked AS INT) AS "Total Items Picked",
     		CAST(PIC.ItemsRequired AS INT) AS "Total Items Required",
     		ROUND(PIC.PickingEfficiencyPerOrder,2) AS "Picking Efficiency Per Order (min)",
     		ROUND(PIC.PickingEfficiencyPerItemPicked,2) AS "Picking Efficiency Per Item Picked (min)",
     		ROUND(PIC.PickingEfficiencyPerItemRequired,2) AS "Picking Efficiency Per Item Required (min)",
     		ROUND(PAC.PackingTime,2) AS "Total Packing Time (min)",
     		PAC.ItemsPacked AS "Total Items Packed",
     		ROUND(PAC.PackingEfficiencyPerItem,2) AS "Packing Efficiency Per Item (min)",
     		PAC.CartonsPacked AS "Total Cartons Packed",
     		PAC.PackingEfficiencyPerCarton AS "Packing Efficiency Per Carton (min)",
     		ROUND((PAC.ItemsPacked  * 13.2) / 60, 2) AS "Additional Time For Items Packed (min)" --* $AdditionalTimeAllocationPerItemPackedInSec = 13.213.2 
     		
     FROM KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.ORDER_MANAGEMENT.FACT_ORDER_DETAIL FOD
     FULL OUTER JOIN KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_LOCATION FS 
     ON FOD.FK_LOCATION_ID = FS.SK_LOCATION_ID
     		
     LEFT JOIN 
    (----------ADDED ON 16/03/23
             -- TO CUT DOWN ADDITIONAL ROWS CAUSED BY GROUP BY STATEMENTS IN THE INNER QUERY
        SELECT STR_ID
            , "DATE"
            , ORDERID
            , FY_PW
            , SUM(PickingTime) AS PickingTime
            , SUM(OrdersPicked) AS OrdersPicked 
            , SUM(ItemsPicked) AS ItemsPicked
            , SUM(ItemsRequired) AS ItemsRequired 
            , AVG(PickingEfficiencyPerOrder) AS PickingEfficiencyPerOrder
            , SUM(PickingEfficiencyPerItemPicked) AS PickingEfficiencyPerItemPicked
            , AVG(PickingEfficiencyPerItemRequired) AS PickingEfficiencyPerItemRequired
        FROM
-----------------
            (SELECT Str_Id,--3
             DD.DATE AS "DATE",
             ORDERID,
              SUM(NEW_PICKINGTIME) AS PickingTime, 
              SUM(NumOfOrders) AS OrdersPicked, 
              SUM(TotalItemsPicked) AS ItemsPicked, 
              SUM(TotalItemsRequired) AS ItemsRequired, 
              concat ('FY', DD.ACCOUNTING_YEAR,
                                'P',
                                RIGHT (DD.ACCOUNTING_PERIOD_DESCRIPTION,2),
                                'W',
                                DD.ACCOUNTING_WEEK_NUMBER
                           ) AS FY_PW,
              CASE
              WHEN SUM(NumOfOrders) = 0 THEN 0
              ELSE ROUND(SUM(NEW_PICKINGTIME)/SUM(NumOfOrders),2)   
              END AS PickingEfficiencyPerOrder,
              CASE
              WHEN SUM(TotalItemsPicked) = 0 THEN 0
              ELSE ROUND((SUM(NEW_PICKINGTIME) OVER (PARTITION BY STR_ID, DD.DATE))/(SUM(TotalItemsPicked) OVER (PARTITION                BY STR_ID, DD.DATE)) ,2)  
              END AS PickingEfficiencyPerItemPicked,
              CASE
              WHEN SUM(TotalItemsRequired) = 0 THEN 0
              ELSE ROUND((SUM(NEW_PICKINGTIME) OVER (PARTITION BY STR_ID, DD.DATE))/(SUM(TotalItemsRequired) OVER                         (PARTITION BY STR_ID, DD.DATE)),2)  
              END AS PickingEfficiencyPerItemRequired
              FROM
              ---------------;
              -- CHANGED ON 16/03/23
              -- THE FOLLOWING WAS CHANGED AS TO RESHARE THE PICKING TIME AT ORDERID LEVEL
                  (SELECT STR_ID,
                      "DATE",
                      PICKING.STOREPICKID,
                      PICKING.ORDERID,
                      PickingTime,
                      NumOfOrders,
                      TotalItemsPicked,
                      TotalItemsRequired,
                      AVG(PICKINGTIME)  OVER (PARTITION BY PICKING.STOREPICKID)/SUM(TotalItemsRequired) OVER (PARTITION BY                        PICKING.STOREPICKID) AS AVG_PICKINGTIME,
                      (TotalItemsRequired * AVG_PICKINGTIME) AS NEW_PICKINGTIME
                  FROM
                      (SELECT DISTINCT ORD.Str_Id,--4
                       TO_DATE(ORD.DO_CREATED) AS "DATE",
                       ORD.StorePickID,
                       ORD.OrderID, 
    
                       COUNT(DISTINCT ORD.OrderID) AS NumOfOrders, 
                       SUM(QtyPicked) AS TotalItemsPicked,
                       SUM(QtyRequired) AS TotalItemsRequired
                      
                       FROM KSFPA.OMS.STOREPICK PCK 
                       INNER JOIN KSFPA.OMS.STOREPICK_ORDERITEMS ORD 
                       ON (PCK.Str_ID = ORD.Str_ID) AND (PCK.StorePickID = ORD.StorePickID)
                      
                       WHERE (PCK.StorePickStatus = 'PickingComplete') 
                       
                       GROUP BY 1,2,3,4
                       
                      ) PICKING
    
                   LEFT JOIN 
                       (SELECT DISTINCT ORD.StorePickID,
                       CASE
                       WHEN (ROUND(CAST(DATEDIFF(second, MIN(PCK.DO_Created), MAX(PCK.DO_Modified)) AS float)/60, 2)) /                            SUM(QtyRequired)>= 5 AND SUM(QtyRequired) > 0 THEN SUM(QtyRequired) --* $DefaultPickingTimePerItem =1
                       WHEN (ROUND(CAST(DATEDIFF(second, MIN(PCK.DO_Created), MAX(PCK.DO_Modified)) AS float)/60, 2)) /                            SUM(QtyRequired) < 0  AND SUM(QtyRequired) > 0 THEN SUM(QtyRequired) --*@DefaultPickingTimePerItem =1
                       ELSE (ROUND(CAST(DATEDIFF(second, MIN(PCK.DO_Created), MAX(PCK.DO_Modified)) AS float)/60, 2))
                       END AS PickingTime
                       FROM KSFPA.OMS.STOREPICK PCK 
                       INNER JOIN KSFPA.OMS.STOREPICK_ORDERITEMS ORD 
                       ON (PCK.Str_ID = ORD.Str_ID) AND (PCK.StorePickID = ORD.StorePickID)
                       GROUP BY 1
                       )T
                  ON PICKING.STOREPICKID = T.STOREPICKID
                  ORDER BY 3,4) P

                  ---------------------------
              LEFT JOIN KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE DD
     		  ON P."DATE" = DD.DATE
              WHERE ACCOUNTING_YEAR >= 2021
              -- ADD THE PERIOD AND WEEK FOR VALIDATION
        	  --AND ACCOUNTING_PERIOD_DESCRIPTION = 'Period 9' OR ACCOUNTING_PERIOD_DESCRIPTION = 'Period 10' OR                          ACCOUNTING_PERIOD_DESCRIPTION = 'Period 11'
              -- OR ACCOUNTING_PERIOD_DESCRIPTION = 'Period 12'
     		  --AND ACCOUNTING_WEEK_NUMBER = 3
              GROUP BY 1,2,3,NEW_PICKINGTIME,TotalItemsPicked,TotalItemsRequired, FY_PW 
              )
              GROUP BY 1,2,3, FY_PW
              ORDER BY 3
             ) PIC ON FOD.DD_ORDERID = PIC.ORDERID AND FS.LOCATION_CODE = PIC.Str_ID 
     
     
     		LEFT JOIN
             (SELECT Str_Id, --3
                   DD.DATE AS "DATE",
              	   ORDERID,
                   SUM(PackingTime) AS PackingTime, 
                   SUM(ItemsPacked) AS ItemsPacked,  
                   SUM(CartonsPacked) AS CartonsPacked,  
                   CASE
                   WHEN (ItemsPacked) = 0 THEN 0
                   ELSE ROUND((SUM(PackingTime) OVER (PARTITION BY STR_ID, DD.DATE))/(SUM(ItemsPacked) OVER (PARTITION BY STR_ID, DD.DATE)),2) --USED IT TO OVERCOME THE ERRORS WHILE AVERAGING THE VALUES  
                   END AS PackingEfficiencyPerItem,
                   CASE
                   WHEN (CartonsPacked) = 0 THEN 0
              	   ELSE ROUND((SUM(PackingTime) OVER (PARTITION BY STR_ID, DD.DATE))/(SUM(CartonsPacked) OVER (PARTITION BY STR_ID, DD.DATE)),2)
                   END AS PackingEfficiencyPerCarton
               	   FROM
                      (SELECT Str_Id, --4
                       	  PACKING."DATE",
                       	  ORDERID,
                          SUM(PACKINGTIME) AS PackingTime, 
                          SUM(CartonNumOfItems) AS ItemsPacked,  
                          COUNT(1) AS CartonsPacked  
                          FROM
                              (SELECT sc.Str_id,--5
                               	  TO_DATE(SC.DO_PACKING) AS "DATE",
                                  OrderID,
                                  (CASE 
                                  WHEN (ROUND(CAST(DATEDIFF(SECOND, sp.DO_Created, sp.DO_Modified) AS float)/60, 2)) > 90 THEN (0.5 * CartonNumOfItems) / (Select Count(1) from KSFPA.OMS.SHIPMENTCARTON where StorePackID = sc.StorePackID) 
                                  -- If the calculated pack time per pack run exceeds the MaxPackingTimePerPackRun = 90, then use the DefaultPackingTimePerItem * CartonNumOfItems
								  ELSE (ROUND(CAST(DATEDIFF(SECOND, sp.DO_Created, sp.DO_Modified) AS float)/60, 2)) / (Select Count(1) from KSFPA.OMS.SHIPMENTCARTON where StorePackID = sc.StorePackID) END) AS PackingTime,  
                                  CartonNumOfItems,
                                  CartonNum 
                                  FROM KSFPA.OMS.SHIPMENTCARTON sc
                                  JOIN KSFPA.OMS.STOREPACK sp 
                                  ON sp.StorePackID = sc.StorePackID
                                  WHERE (COALESCE(CartonNumOfItems, 0) > 0) 
                                  AND COALESCE(sc.StorePackID,0) > 0
                               ) PACKING GROUP BY 1,2,3
                      ) AS PackingTemp  
              LEFT JOIN KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE DD
     		  ON PACKINGTEMP."DATE" = DD.DATE
              WHERE ACCOUNTING_YEAR >= 2021
               -- ADD THE PERIOD AND WEEK FOR VALIDATION
               --AND ACCOUNTING_PERIOD_DESCRIPTION = 'Period 9' 
               --OR ACCOUNTING_PERIOD_DESCRIPTION = 'Period 10' 
               --OR ACCOUNTING_PERIOD_DESCRIPTION = 'Period 11'
               --OR ACCOUNTING_PERIOD_DESCRIPTION = 'Period 12'
     		   --AND ACCOUNTING_WEEK_NUMBER = 3
              GROUP BY 1,2,3,PackingTime,ItemsPacked,CartonsPacked
             ) PAC ON FOD.DD_ORDERID = PAC.ORDERID AND FS.LOCATION_CODE = PAC.Str_ID AND PIC."DATE" <= PAC."DATE" 
     
     	WHERE FS.LOCATION_CODE NOT IN (1000)
     	AND FS.TRADING_STATUS = 'Open'

    ) Total
 JOIN (SELECT DISTINCT(ORDERID),CUSTOMERORDERTYPE FROM KSFPA.OMS.CUSTOMERORDER) CO
 ON TOTAL."Order ID" = CO.ORDERID

 --WHERE (TOTAL_PICKING_TIME_MIN OR
 --   TOTAL_ORDERS_PICKED OR
 --   TOTAL_ITEMS_PICKED OR
 --   TOTAL_ITEMS_REQUIRED OR
 --   TOTAL_PACKING_MIN OR
 --   TOTAL_ITEMS_PACKED OR
 --   TOTAL_CARTONS_PACKED OR
 --   PICKING_EFFICIENCY_PER_ORDER_MIN OR
 --   PICKING_EFFICIENCY_PER_ITEM_PICKED_MIN OR
 --   PICKING_EFFICIENCY_PER_ITEM_REQUIRED_MIN OR
 --   PACKING_EFFICIENCY_PER_ITEM_MIN OR
 --   PACKING_EFFICIENCY_PER_CARTON_MIN OR
 --   ADDITIONAL_TIME_FOR_ITEMS_PACKED_MIN OR
 --   TOTAL_FULFILLMENT_TIME_MIN) IS NOT NULL
    
GROUP BY FY_PW,"Store", "Store Name", CO.CUSTOMERORDERTYPE, ORDER_DATE, State
ORDER BY FY_PW,1,2,3,4,5 desc;