create or replace view KSFPA.ONLINE_UGAM_PVT.CANCELLED_ORDERS_MAIN1(
	YEAR,
	PERIOD,
	WEEK,
	FY_PERIOD_WEEK,
	CANCELLED_DATE,
	COST,
	ORDER_COUNT,
	MOVING_COST,
	VAR,
	AVG_4_WEEKS,
	MOVING_COST_4_WEEKS,
	VAR_4_WEEKS,
	RETURN_REASON
) as 
SELECT 
ACCOUNTING_YEAR AS YEAR,
    ACCOUNTING_MONTH_NUMBER AS PERIOD,
    ACCOUNTING_WEEK_NUMBER AS WEEk,
CONCAT('FY',RIGHT(ACCOUNTING_YEAR,2),'P',ACCOUNTING_MONTH_NUMBER,'W',ACCOUNTING_WEEK_NUMBER) AS FY_PERIOD_WEEK,
Cancelled_Date,
cost,
Order_Count,
moving_cost,
var,
avg_4_weeks,
moving_cost_4_weeks,
var_4_weeks,
Return_Reason
FROM
(
select 
Cancelled_Date,
cost as cost,
Order_Count,
avg(cost) over (partition by Return_Reason order by Cancelled_Date rows between 84 preceding and 1 preceding) as moving_cost,
(cost/avg(cost) over (partition by Return_Reason order by Cancelled_Date rows between 84 preceding and 1 preceding))-1 as var,
avg(cost) over (partition by Return_Reason order by Cancelled_Date rows between 29 preceding and 1 preceding) as avg_4_weeks,
avg(cost) over (partition by Return_Reason order by Cancelled_Date rows between 28 preceding and 1 preceding) as moving_cost_4_weeks,
(cost/avg(cost) over (partition by Return_Reason order by Cancelled_Date rows between 28 preceding and 1 preceding))-1 as var_4_weeks,

Return_Reason
from
((

select 
Cancelled_Date,
sum(cost) as cost,
COUNT(DISTINCT(ExternalOrderID)) as Order_Count,
Return_Reason

FROM 
(
Select soi.ExternalOrderID,
soi.UnitPrice * soi.Quantity as Cost,
DATE(soi.DO_Cancelled) AS Cancelled_Date,
CASE WHEN DATEDIFF(minute, soi.OrderDate, soi.DO_Created) < 45 
AND (ParentShipmentID = '0' OR ParentShipmentID is null) THEN 'Upfront Rejection (HD)' 
			ELSE 'HD Store Rejection (Exception)' 
			END AS Return_Reason
from "KSFPA"."OMS"."STOREORDERITEMS" soi
WHERE
  soi.DO_Created > '2020-06-29 00:00:00'
  and soi.DO_Created <  CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  and soi.str_id ='9999'
  and soi.DO_Cancelled is not null
  and soi.DO_Cancelled < CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  
UNION ALL
  
Select soi.ExternalOrderID, 
soi.UnitPrice * soi.Quantity as Cost, 
DATE(soi.DO_Cancelled) AS Cancelled_Date ,
CASE WHEN LTRIM(RTRIM(ModificationDescription)) = 'Cancelled'  
      THEN (CASE WHEN DATEDIFF(minute, soi.OrderDate, soi.DO_Created) < 45 
      AND (ParentShipmentID = '0' OR ParentShipmentID is null) THEN 'Upfront Rejection (CC)'
      ELSE 'C&C Store Rejection (Exception)' END)
ELSE 'Click & Collect Store Rejection - Customer chose free delivery'
			END AS Return_Reason
from "KSFPA"."OMS"."STOREORDERITEMS" soi
WHERE
  soi.DO_Created > '2020-06-29 00:00:00'
  and soi.DO_Created < CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  and soi.str_id ='9996'
  and soi.DO_Cancelled is not null
  and soi.DO_Cancelled < CONCAT(CURRENT_DATE()-1, ' 23:59:59')

UNION ALL
  
Select soi.ExternalOrderID, 
soi.UnitPrice * soi.Quantity as Cost, 
DATE(soi.DO_Cancelled) AS Cancelled_Date, 
'Ready to Collect Order Not Collected' AS Return_Reason
from  "KSFPA"."OMS"."STOREORDERITEMS" soi
WHERE
  soi.DO_Cancelled > '2020-06-29 00:00:00'
  and soi.DO_Cancelled < CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  and LTRIM(RTRIM(soi.ShipStatus)) = 'ItemCancelled'
  and QuantityReadyToCollect > '0'
  and soi.DO_Cancelled is not null
) 
group by Cancelled_Date,Return_Reason
order by Cancelled_Date
)
UNION ALL 
(

select 
DISTINCT Cancelled_Date,
sum(cost) as cost,
COUNT(DISTINCT(ExternalOrderID)) as Order_Count,
'Total' AS Return_Reason

FROM 
(
Select soi.ExternalOrderID,
soi.UnitPrice * soi.Quantity as Cost,
DATE(soi.DO_Cancelled) AS Cancelled_Date,
CASE WHEN DATEDIFF(minute, soi.OrderDate, soi.DO_Created) < 45 
AND (ParentShipmentID = '0' OR ParentShipmentID is null) THEN 'Upfront Rejection (HD)' 
			ELSE 'HD Store Rejection (Exception)' 
			END AS Return_Reason
from "KSFPA"."OMS"."STOREORDERITEMS" soi
WHERE
  soi.DO_Created > '2020-06-29 00:00:00'
  and soi.DO_Created <  CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  and soi.str_id ='9999'
  and soi.DO_Cancelled is not null
  and soi.DO_Cancelled < CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  
UNION ALL
  
Select soi.ExternalOrderID, 
soi.UnitPrice * soi.Quantity as Cost, 
DATE(soi.DO_Cancelled) AS Cancelled_Date ,
CASE WHEN LTRIM(RTRIM(ModificationDescription)) = 'Cancelled'  
      THEN (CASE WHEN DATEDIFF(minute, soi.OrderDate, soi.DO_Created) < 45 
      AND (ParentShipmentID = '0' OR ParentShipmentID is null) THEN 'Upfront Rejection (CC)'
      ELSE 'C&C Store Rejection (Exception)' END)
ELSE 'Click & Collect Store Rejection - Customer chose free delivery'
			END AS Return_Reason
from "KSFPA"."OMS"."STOREORDERITEMS" soi
WHERE
  soi.DO_Created > '2020-06-29 00:00:00'
  and soi.DO_Created < CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  and soi.str_id ='9996'
  and soi.DO_Cancelled is not null
  and soi.DO_Cancelled < CONCAT(CURRENT_DATE()-1, ' 23:59:59')

UNION ALL
  
Select soi.ExternalOrderID, 
soi.UnitPrice * soi.Quantity as Cost, 
DATE(soi.DO_Cancelled) AS Cancelled_Date ,
'Ready to Collect Order Not Collected' AS Return_Reason
from  "KSFPA"."OMS"."STOREORDERITEMS" soi
WHERE
  soi.DO_Cancelled > '2020-06-29 00:00:00'
  and soi.DO_Cancelled < CONCAT(CURRENT_DATE()-1, ' 23:59:59')
  and LTRIM(RTRIM(soi.ShipStatus)) = 'ItemCancelled'
  and QuantityReadyToCollect > '0'
  and soi.DO_Cancelled is not null
) 
group by Cancelled_Date
order by Cancelled_Date
))
) 
T1
LEFT JOIN KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE DD ON T1.Cancelled_Date = DD.DATE
ORDER BY 5 DESC;