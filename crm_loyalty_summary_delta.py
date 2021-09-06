# Databricks notebook source
# spark.conf.set('setting spark.sql.autoBroadcastJoinThreshold', 36000000)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from datetime import datetime
from delta.tables import *

update_date = datetime.strptime(dbutils.widgets.get("update_dt"), '%Y-%m-%d %H:%M:%S')
file_dt = datetime.strftime(update_date, '%Y%m%d%H%M%S')
print("update date is : {0}".format(update_date))
print("file date is : {0}".format(file_dt))

# COMMAND ----------

env = dbutils.widgets.get('env')

if env == 'QA':
  db = 'qa_apps'
  apps_path = 'gs://petm-bdpl-qa-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  table_name = 'crm_loyalty_summary'
else:
  db = 'apps'
  apps_path = 'gs://petm-bdpl-prod-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  table_name = 'crm_loyalty_summary'

table_path = apps_path+"/SFMC/delta_tables/"+table_name
file = apps_path+"/SFMC/data_feeds/"+table_name+"/Loyalty_Summary_"+file_dt
table = db+"."+table_name

print ("Output Table : "+table)
print ("Output File : "+file)

# COMMAND ----------

spark.sql("""select a.customer_eid , a.pet_eid from (select cp.customer_eid,p.pet_eid,
  row_number() OVER (
    PARTITION BY 
    IF(
            (trim(p.pet_name) != ''),
            IF(
              upper(regexp_replace(p.pet_name, "[^a-zA-Z0-9]", "")) != '',
              upper(regexp_replace(p.pet_name, "[^a-zA-Z0-9] ", "")),
              'BLANK'
            ),
            'BLANK'
         ),
    cp.customer_eid,
    nvl(p.species,'') ,
    nvl(regexp_replace(p.breed_description, "\\\\s+", " "),'') 
    ORDER BY
      p.update_dt_tm,
      p.pet_eid DESC
  ) rownum
        from refine.cdm_pet p
        inner join refine.cdm_customer_pet cp
        on p.pet_eid = cp.pet_eid
        where cp.active_flag = 1) a where a.rownum = 1
        order by customer_eid""").createOrReplaceTempView('pet_count_table')

# COMMAND ----------

crm_loyalty_summary = spark.sql("""SELECT 
cc.customer_eid as EID, 
cc.loyalty_nbr as LoyaltyNbr,
nvl(cc.profile_complete_flag,false) as ProfileCompletionFlag, 
nvl(cc.enrollment_country,'') as CountryOfEnrollment,
date_format(nvl(aimia.enrollment_date,''), "yyyy-MM-dd HH:mm:ss") as EnrollmentDttm,
nvl(aimia.enrollment_location,'') as StoreOfEnrollment,
nvl(pct.pet_count, 0) as TotalPets,
cc.registration_channel as RegistrationChannel,
cc.member_descriptor as MemberDescriptor,
aimia.ranking_set as RankingSet,
aimia.ranking_set_level as RankingSetLevel,
date_format(nvl(aimia.ranking_set_level_join_date,''), "yyyy-MM-dd HH:mm:ss") as RankingSetLevelJoinDate,
date_format(nvl(aimia.ranking_member_descriptor_expiration_date,''), "yyyy-MM-dd HH:mm:ss") as RankingMemberDescriptorExpirationDate,
date_format(to_timestamp('"""+file_dt+"""',"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss") as file_dt_tm
FROM (
  SELECT distinct customer_eid, enrollment_country, profile_complete_flag, loyalty_nbr, update_dt_tm, registration_channel, member_descriptor
  FROM refine.cdm_customer 
  WHERE loyalty_nbr != 0
  ) cc
INNER JOIN 
(
  SELECT ama.member_account_id, am.enrollment_date,am.enrollment_location,amr.ranking_set,amr.ranking_set_level,amr.ranking_set_level_join_date,amr.ranking_member_descriptor_expiration_date,
  row_number() over (partition by ama.member_account_id order by am.standard_update_tstmp desc) row_num 
  FROM  refine.aimia_member_account ama
  inner join refine.aimia_member am 
  on ama.internal_member_id=am.internal_member_id 
  left join refine.aimia_member_ranking amr
  on am.internal_member_id = amr.internal_member_id
  where ama.id_type_external_reference='LoyaltyID'
) aimia
ON aimia.member_account_id=cc.loyalty_nbr AND aimia.row_num==1
LEFT JOIN (
  SELECT distinct customer_eid, count(pet_eid) over (partition by customer_eid) pet_count
  FROM pet_count_table
  ) pct
ON cc.customer_eid = pct.customer_eid
WHERE cc.update_dt_tm = '{0}'
""".format(update_date))

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
crm_loyalty_summary.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

#crm_loyalty_summary.write.format('delta').mode("overwrite").option("path", table_path).option("overwriteSchema", "true").saveAsTable(table)

# COMMAND ----------

crm_loyalty_summary_cnt = crm_loyalty_summary.count()
if crm_loyalty_summary_cnt > 0 :
  deltaTable = DeltaTable.forPath(spark, table_path)
  deltaTable.alias("base").merge(crm_loyalty_summary.alias("pre"), "base.eid = pre.eid") \
     .whenNotMatchedInsertAll() \
     .whenMatchedUpdateAll() \
     .execute()
  print ("Table "+ table +" has ben loaded with " +str(crm_loyalty_summary_cnt)+ " records")
  crm_loyalty_summary.drop("file_dt_tm").coalesce(1).write.format('csv') \
               .mode('overwrite') \
               .option('header','true') \
               .option('delimiter','|') \
               .option('escapeQuotes', 'false') \
               .option("emptyValue","") \
               .option('maxRecordsPerFile', '12000000') \
               .save(file)
else:
  raise Exception('Zero Records')

# COMMAND ----------

crm_loyalty_summary.unpersist()
