# Databricks notebook source
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled","true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",'50M')

# COMMAND ----------

import pandas as pd
from datetime import date,datetime
from delta.tables import *
from pyspark.sql.functions import *

curr_dt_tm = pd.Timestamp.now(tz='MST').strftime('%Y%m%d%H%M%S') #get current date
print("file name date is : " + curr_dt_tm)
start_dt = datetime.strptime(dbutils.widgets.get("start_dt")[:19],'%Y-%m-%d %H:%M:%S')
end_dt = datetime.strptime(dbutils.widgets.get("end_dt")[:19],'%Y-%m-%d %H:%M:%S')
print("start date is : " + str(start_dt))
print("end date is : " + str(end_dt))

# COMMAND ----------

env = dbutils.widgets.get('env')

if env == 'QA':
  db = 'qa_apps'
  apps_path = 'gs://petm-bdpl-qa-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  table_name = 'crm_transaction_details'
else:
  db = 'apps'
  apps_path = 'gs://petm-bdpl-prod-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  table_name = 'crm_transaction_details'

table_path = apps_path+"/SFMC/delta_tables/"+table_name
file = apps_path+"/SFMC/data_feeds/"+table_name+"/Transaction_Details_"+curr_dt_tm
table = db+"."+table_name

print ("Output Table : "+table)
print ("Output File : "+file)

# COMMAND ----------

sales_trans_sku_subset =spark.sql("""
SELECT sts.sales_instance_id, 
       sts.day_dt,
       sts.customer_eid,
       sts.product_id, 
       sts.location_id, 
       sts.loyalty_redemption_id, 
       sts.sales_amt, 
       sts.sales_qty, 
       sts.return_amt, 
       sts.return_qty, 
       sts.discount_amt, 
       sts.discount_qty, 
       sts.discount_return_amt, 
       sts.discount_return_qty, 
       sts.clearance_amt, 
       sts.clearance_qty, 
       sts.clearance_return_amt, 
       sts.clearance_return_qty, 
       sts.pos_coupon_amt, 
       sts.pos_coupon_qty, 
       sts.net_sales_amt, 
       sts.net_margin_amt, 
       sts.exch_rate_pct, 
       sts.load_tstmp, 
       sts.update_tstmp 
FROM   refine.sales_trans_sku sts 
WHERE  day_dt >= (DATE('{1}') - interval 90 days)
       and update_tstmp >= '{0}' AND update_tstmp < '{1}'
       and sales_instance_id is not null 
""".format(start_dt, end_dt))
sales_trans_sku_subset.write.format('delta') \
             .mode("overwrite") \
             .option("path", work_path+"/crm/crm_sales_tran_sku_subset") \
              .option("overwriteSchema", "true") \
              .saveAsTable("work.sales_trans_sku_subset")

# COMMAND ----------

#table_creation
crm_transaction_details = spark.sql("""
SELECT /*+ BROADCAST(sku), BROADCAST(st) */
       sts.sales_instance_id AS TransactionHeaderId, 
       sts.day_dt                     AS TransactionDttm,
       sts.customer_eid               AS EID,
       sku.sku_nbr                    AS SKU, 
       st.site_nbr                    AS StoreNbr, 
       sts.loyalty_redemption_id      AS LoyaltyRedemptionId, 
       sts.sales_amt                  AS SalesAmount, 
       sts.sales_qty                  AS SalesQuantity, 
       sts.return_amt                 AS ReturnAmount, 
       sts.return_qty                 AS ReturnQuantity, 
       sts.discount_amt               AS DiscountAmount, 
       sts.discount_qty               AS DiscountQuantity, 
       sts.discount_return_amt        AS DiscountReturnAmount, 
       sts.discount_return_qty        AS DiscountReturnQuantity, 
       sts.clearance_amt              AS ClearenceAmount, 
       sts.clearance_qty              AS ClearenceQuantity, 
       sts.clearance_return_amt       AS ClearenceReturnAmount, 
       sts.clearance_return_qty       AS ClearenceReturnQuantity, 
       sts.pos_coupon_amt             AS CouponAmount, 
       sts.pos_coupon_qty             AS CouponQuantity, 
       sts.net_sales_amt              AS NetSalesAmount, 
       sts.net_margin_amt             AS NetMarginAmount, 
       sts.exch_rate_pct              AS ExchangeRatePCT, 
       sts.load_tstmp                 AS CreateDttm, 
       sts.update_tstmp               AS UpdateDttm ,
       date_format(to_timestamp('"""+curr_dt_tm+"""',"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss") as file_dt_tm
FROM   work.sales_trans_sku_subset sts 
       INNER JOIN refine.sku_profile_dl sku
               ON sts.product_id = sku.product_id 
       INNER JOIN refine.sap_site_master st
               ON sts.location_id = st.location_id """)

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
crm_transaction_details.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

#crm_transaction_details.write.format('delta').mode("overwrite").option("path", table_path).option("overwriteSchema", "true").saveAsTable(table)

# COMMAND ----------

crm_transaction_details_cnt = crm_transaction_details.count()
if crm_transaction_details_cnt > 0 :
  deltaTable = DeltaTable.forPath(spark, table_path)
  deltaTable.alias("base").merge(crm_transaction_details.alias("pre"), "base.SKU = pre.SKU and base.TransactionHeaderId = pre.TransactionHeaderId") \
     .whenNotMatchedInsertAll() \
     .whenMatchedUpdateAll() \
     .execute()
  print ("Table "+ table +" has ben loaded with " +str(crm_transaction_details_cnt)+ " records")
  crm_transaction_details.drop("file_dt_tm").coalesce(1).write.format('csv') \
               .mode('overwrite') \
               .option('header','true') \
               .option('delimiter','|') \
               .option('escapeQuotes', 'false') \
               .option("emptyValue","") \
               .save(file)
else:
  raise Exception('Zero Records')

# COMMAND ----------

crm_transaction_details.unpersist()
