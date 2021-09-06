# Databricks notebook source
# MAGIC %md 
# MAGIC History load done between 2019-01-01 00:00:00 to 2021-03-29 15:00:01

# COMMAND ----------

# spark.conf.set('setting spark.sql.autoBroadcastJoinThreshold', 36000000)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from datetime import datetime
from delta.tables import *

start_dt = datetime.strptime(dbutils.widgets.get("start_dt"), '%Y-%m-%d %H:%M:%S')
end_dt = datetime.strptime(dbutils.widgets.get("end_dt"), '%Y-%m-%d %H:%M:%S')
file_dt = datetime.strftime(end_dt, '%Y%m%d%H%M%S')
print("start_dt date is : {0} end_dt is: {1}".format(start_dt, end_dt))
print("file date is : {0}".format(file_dt))

# COMMAND ----------

env = dbutils.widgets.get('env')

if env == 'QA':
  db = 'qa_apps'
  apps_path = 'gs://petm-bdpl-qa-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  table_name = 'crm_service_booking'
else:
  db = 'apps'
  apps_path = 'gs://petm-bdpl-prod-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  table_name = 'crm_service_booking'

table_path = apps_path+"/SFMC/delta_tables/"+table_name
file = apps_path+"/SFMC/data_feeds/"+table_name+"/Service_Booking_"+file_dt
table = db+"."+table_name

print ("Output Table : "+table)
print ("Output File : "+file)

# COMMAND ----------

# MAGIC   %python
# MAGIC #Considerations & Assumptions
# MAGIC # There are 0 records for query which we are mapping for ServicesRedemptionId(select * from refine.svcs_work_order where svcs_work_order_psvc_redemption is not null)
# MAGIC # No records for RequestedEmployeeName reference query -> select ssr2.svcs_service_resource_name from refine.svcs_work_order swo join refine.svcs_service_resource  ssr2 on ssr2.svcs_service_resource_id = # swo.svcs_work_order_psvc_required_resource
# MAGIC # There are service appointments without POSInvoiceId, Sku, TotalPrice etc. after left join
# MAGIC # Incorrect mapping for svcs_service_appointment_status considering CANCELED
# MAGIC from pyspark.sql.functions import date_format
# MAGIC from pyspark.sql import functions as f
# MAGIC crm_service_booking = spark.sql("""SELECT
# MAGIC   cu.customer_eid as EID,
# MAGIC   'PRISM' as  ServicesSourceCD,
# MAGIC    nvl(ssa.svcs_service_appointment_id,'') as ServicesAppointmentId,
# MAGIC    nvl(swo.svcs_work_order_work_order_number,'') as AppointmentNbr,
# MAGIC    nvl(ssa.svcs_service_appointment_appointment_number,'') as ServicesBookingNbr,
# MAGIC    nvl(si.svcs_invoice_psvc_invoice_number, '') as POSInvoiceId,
# MAGIC    nvl(date_format(from_utc_timestamp(si.svcs_invoice_created_date, "America/Phoenix") , 'yyyy-MM-dd HH:mm:ss'),'') as InvoiceCreateDttm,
# MAGIC    nvl(so.svcs_order_id,'') as ServicesOrderId,
# MAGIC    nvl(so.svcs_order_order_number,'') as WebOrderNbr,
# MAGIC    nvl(swo.svcs_work_order_psvc_redemption,'') as ServicesRedemptionId,
# MAGIC    nvl(so.svcs_order_psvc_store_number,'') as StoreNbr,
# MAGIC    nvl(date_format(from_utc_timestamp(ssa.svcs_service_appointment_due_date, "America/Phoenix"), 'yyyy-MM-dd HH:mm:ss'),'') as AppointmentDttm,
# MAGIC    nvl(ssa.svcs_service_appointment_status,'') as AppointmentStatus,
# MAGIC    concat('PSVC_', ssa.svcs_service_appointment_status ) as SourceStatus,
# MAGIC    CASE 
# MAGIC      WHEN UPPER(ssa.svcs_service_appointment_psvc_creation_channel)='STORE' OR  UPPER(ssa.svcs_service_appointment_psvc_creation_channel)='IN-STORE' THEN 'IN-STORE'
# MAGIC      WHEN UPPER(ssa.svcs_service_appointment_psvc_creation_channel)='ONLINE' OR  UPPER(ssa.svcs_service_appointment_psvc_creation_channel)='ONLINE MOBILE' THEN 'DIGITAL'
# MAGIC      WHEN UPPER(ssa.svcs_service_appointment_psvc_creation_channel)='SRC' THEN 'SRC'
# MAGIC      ELSE 'OTHER'
# MAGIC    END AS AppointmentChannel,
# MAGIC    nvl(so.svcs_order_psvc_void_reason,'') as OrderVoidReason,
# MAGIC    nvl(date_format(from_utc_timestamp(so.svcs_order_psvc_void_date_time, "America/Phoenix"), 'yyyy-MM-dd HH:mm:ss'),'') as OrderVoidDttm,
# MAGIC    'Grooming' as ServicesCategory,
# MAGIC    nvl(asset.svcs_asset_psvc_pods_pet_id,'') as PetEID,
# MAGIC    nvl(svp.svcs_product_stock_keeping_unit,'') as SKU, 
# MAGIC    sku.sku_desc as SKUDesc,
# MAGIC    case when sku.sap_class_id in (812,813) then '1'
# MAGIC         when sku.sap_class_id in (811) then '0'
# MAGIC         else '' end as PrimaryFlag,
# MAGIC    IF(UPPER(ssa.svcs_service_appointment_status)=='CANCELED',1, 0) as CancelledFlag,
# MAGIC    IF(UPPER(ssa.svcs_service_appointment_status)=='CANCELED' OR UPPER(swo.svcs_work_order_psvc_cancellation_reason)=='NO-SHOW',1, 0) as NoShowFlag,
# MAGIC    '0' as RebookFlag,
# MAGIC    IF(((bigint(ssa.svcs_service_appointment_sched_start_time)-bigint(ssa.svcs_service_appointment_created_date))/60)<5,1,0)  as WalkInFlag,
# MAGIC    IF(soi.svcs_order_item_total_price is null, format_number((soi.svcs_order_item_quantity * soi.svcs_order_item_unit_price),2) , format_number(soi.svcs_order_item_total_price,2)) as TotalPrice,
# MAGIC    nvl(soi.svcs_order_item_quantity,'') as Quantity,
# MAGIC    CONCAT(swo.svcs_work_order_psvc_staff_first_name, " ", swo.svcs_work_order_psvc_staff_last_name) as AssignedEmployeeName,
# MAGIC    nvl(ssr.svcs_service_resource_name,'') as RequestedEmployeeName,
# MAGIC    nvl(date_format(from_utc_timestamp(ssa.svcs_service_appointment_created_date, "America/Phoenix"), 'yyyy-MM-dd HH:mm:ss'),'') as AppointmentCreateDttm,
# MAGIC    nvl(date_format(from_utc_timestamp(ssa.svcs_service_appointment_last_modified_date, "America/Phoenix"), 'yyyy-MM-dd HH:mm:ss'),'') as AppointmentUpdateDttm,
# MAGIC    soi.svcs_order_item_hard_delete_flag as HardDeleteFlag,
# MAGIC    date_format(to_timestamp('"""+file_dt+"""',"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss") as file_dt_tm
# MAGIC    
# MAGIC    FROM refine.cdm_customer cu
# MAGIC   
# MAGIC    JOIN refine.svcs_account sa
# MAGIC    ON cu.person_id = sa.svcs_account_psvc_pods_customer_id
# MAGIC   
# MAGIC    JOIN refine.svcs_service_appointment ssa
# MAGIC    ON sa.svcs_account_id = ssa.svcs_service_appointment_account_id
# MAGIC    AND ssa.svcs_update_tstmp >= '{0}' AND ssa.svcs_update_tstmp < '{1}'
# MAGIC    
# MAGIC    JOIN refine.svcs_work_order swo
# MAGIC    ON swo.svcs_work_order_id=ssa.svcs_service_appointment_psvc_work_order
# MAGIC    
# MAGIC    JOIN refine.svcs_order so
# MAGIC    ON so.svcs_order_id=swo.svcs_work_order_psvc_order
# MAGIC    
# MAGIC    LEFT JOIN refine.svcs_invoice si
# MAGIC    ON si.svcs_invoice_id=so.svcs_order_psvc_invoice
# MAGIC    
# MAGIC    JOIN
# MAGIC    (
# MAGIC      SELECT oi.*, count(svcs_order_item_psvc_work_order, svcs_order_item_product_2_id) OVER (PARTITION BY svcs_order_item_psvc_work_order, svcs_order_item_product_2_id 
# MAGIC      ORDER BY svcs_order_item_created_date DESC) instance_count FROM refine.svcs_order_item oi ) soi
# MAGIC    ON soi.svcs_order_item_psvc_work_order = swo.svcs_work_order_id AND soi.instance_count=1
# MAGIC    
# MAGIC    JOIN refine.svcs_product svp
# MAGIC    ON svp.svcs_product_id=soi.svcs_order_item_product_2_id
# MAGIC    
# MAGIC    JOIN refine.svcs_asset asset
# MAGIC    ON asset.svcs_asset_id = swo.svcs_work_order_asset_id
# MAGIC    
# MAGIC    left JOIN refine.sap_sku_master sku
# MAGIC    ON svp.svcs_product_stock_keeping_unit = sku.sku_nbr
# MAGIC    
# MAGIC    LEFT JOIN refine.svcs_service_resource  ssr
# MAGIC    ON ssr.svcs_service_resource_id = swo.svcs_work_order_psvc_required_resource
# MAGIC    
# MAGIC """.format(start_dt, end_dt))

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
crm_service_booking.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

#crm_service_booking.write.format('delta').mode("overwrite").option("path", table_path).option("overwriteSchema", "true").saveAsTable(table)

# COMMAND ----------

crm_service_booking_cnt = crm_service_booking.count()
if crm_service_booking_cnt > 0 :
  deltaTable = DeltaTable.forPath(spark, table_path)
  deltaTable.alias("base").merge(crm_service_booking.alias("pre"), "base.ServicesAppointmentId = pre.ServicesAppointmentId and base.EID = pre.EID and base.PetEID = pre.PetEID and base.SKU = pre.SKU") \
     .whenNotMatchedInsertAll() \
     .whenMatchedUpdateAll() \
     .execute()
  print ("Table "+ table +" has ben loaded with " +str(crm_service_booking_cnt)+ " records")
  crm_service_booking.drop("file_dt_tm").coalesce(1).write.format('csv') \
               .mode('overwrite') \
               .option('header','true') \
               .option('delimiter','|') \
               .option('escapeQuotes', 'false') \
               .option("emptyValue","") \
               .save(file)
else:
  raise Exception('Zero Records')

# COMMAND ----------

crm_service_booking.unpersist()
