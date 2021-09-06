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

dbutils.widgets.text('env','QA')
env = dbutils.widgets.get('env')

if env == 'QA':
  db = 'qa_apps'
  apps_path = 'gs://petm-bdpl-qa-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-qa-work-p1-gcs-gbl'
  table_name = 'crm_pet'
else:
  db = 'apps'
  apps_path = 'gs://petm-bdpl-prod-apps-p1-gcs-gbl'
  work_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  refine_path = 'gs://petm-bdpl-prod-work-p1-gcs-gbl'
  table_name = 'crm_pet'

table_path = apps_path+"/SFMC/delta_tables/"+table_name
file = apps_path+"/SFMC/data_feeds/"+table_name+"/Pet_"+file_dt
table = db+"."+table_name

print ("Output Table : "+table)
print ("Output File : "+file)

# COMMAND ----------

crm_pet = spark.sql("""select c.customer_eid as EID
        ,cp.pet_eid as PetEID
        ,nvl(lower((cp.active_flag AND p.active_flag)),'') as ActiveFlag
        ,IF(
            (trim(p.pet_name) != ''),
            IF(
              upper(regexp_replace(p.pet_name, "[^a-zA-Z0-9]", "")) != '',
              upper(regexp_replace(p.pet_name, "[^a-zA-Z0-9] ", "")),
              'BLANK'
            ),
            'BLANK'
         ) as PetName
        ,if(p.birth_date ='0001-01-01 00:00:00','',nvl(p.birth_date,'')) as BirthDate
        ,nvl(p.species,'') as SpeciesDescription
        ,nvl(lower(p.species_is_first_party),'') as SpeciesIsFirstParty
        ,nvl(regexp_replace(p.breed_description, "\\\\s+", " "),'') as BreedDescription
        ,nvl(lower(p.breed_is_aggressive),'') as BreedIsAgressive
        ,nvl(lower(p.mixed_breed_flag),'') as MixedBreedFlag
        ,nvl(p.gender,'') as Gender
        ,nvl(regexp_replace(p.color_desc, "\\\\s+", " "),'') as ColorDescription
        ,nvl(regexp_replace(p.pet_markings, "\\\\s+", " "),'') as Markings
        ,nvl(p.weight,'') as Weight
        ,nvl(lower(p.child_flag),'') as ChildFlag
        ,nvl(lower(p.spayed_neutered_flag),'') as SpayedNeuteredFlag
        ,nvl(lower(p.adoption_flag),'') as AdoptionFlag
        ,'' as AdoptionDate
        ,nvl(regexp_replace(p.service_restrictions, "\\\\s+", " "),'') as ServiceRestrictions
        ,nvl(regexp_replace(p.medical_conditions,  "\\\\s+", " "),'') as MedicalConditions
        ,nvl(lower(p.do_not_book_flag),'')  as DoNotBookFlag
        ,lower(p.do_not_book_reason) as DoNotBookReason
        ,'{0}' as CreateDttm
        ,'{0}' as UpdateDttm
        ,p.update_dt_tm as pet_update_tm
        ,date_format(to_timestamp('"""+file_dt+"""',"yyyyMMddHHmmss"),"yyyy-MM-dd HH:mm:ss") as file_dt_tm
        from refine.cdm_customer c 
        inner join refine.cdm_customer_pet cp
        on c.customer_eid = cp.customer_eid
        inner join refine.cdm_pet p
        on cp.pet_eid = p.pet_eid
        WHERE c.update_dt_tm = '{0}' OR
        p.update_dt_tm <= '{0}' OR
        cp.update_dt_tm <= '{0}'
        """.format(update_date))
crm_pet.createOrReplaceTempView('crm_pet_interim')

# COMMAND ----------

# MAGIC %md ####Dedup based on :
# MAGIC * CUSTOMER_EID
# MAGIC * PET_NAME
# MAGIC * SPECIES
# MAGIC * BREED_DESCRIPTION

# COMMAND ----------

crm_pet_interim = spark.sql("""select
  *,
  row_number() OVER (
    PARTITION BY PetName,
    EID,
    SpeciesDescription,
    BreedDescription
    ORDER BY
      pet_update_tm,
      peteid DESC
  ) rownum
from
  crm_pet_interim""")

# COMMAND ----------

crm_pet_final = crm_pet_interim.where('rownum=1').drop('rownum', 'pet_update_tm')

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
crm_pet_final.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

#crm_pet_final.write.format('delta').mode("overwrite").option("path", table_path).option("overwriteSchema", "true").saveAsTable(table)

# COMMAND ----------

crm_pet_final_cnt = crm_pet_final.count()
if crm_pet_final_cnt > 0 :
  deltaTable = DeltaTable.forPath(spark, table_path)
  deltaTable.alias("base").merge(crm_pet_final.alias("pre"), "base.eid = pre.eid and base.peteid = pre.peteid") \
     .whenNotMatchedInsertAll() \
     .whenMatchedUpdateAll() \
     .execute()
  print ("Table "+ table +" has ben loaded with " +str(crm_pet_final_cnt)+ " records")
  crm_pet_final.drop("file_dt_tm").coalesce(1).write.format('csv') \
               .mode('overwrite') \
               .option('header','true') \
               .option('delimiter','|') \
               .option('escapeQuotes', 'false') \
               .option("emptyValue","") \
               .save(file)
else:
  raise Exception('Zero Records')

# COMMAND ----------

crm_pet_final.unpersist()
