# Databricks notebook source
# DBTITLE 1,Reading source files
import ast

metadata_value = dbutils.widgets.get('source_files_name') ##Accessing the output from Get Metadata and storing into a variable using databricks widgets.
#metadata_value = "companies_sorted.csv" 
metadata_list = ast.literal_eval(metadata_value)

blob_output_list=[] ##Creating an empty list to add the names of files we get from GetMetadata activity.
for i in metadata_list:
    #for j in i.items():
    #print(type(i))
            #blob_output_list.append(j) ##This will add all the names of files from blob storage to the empty list we created above.
    c = list(i.items())[0][1]

# COMMAND ----------

# DBTITLE 1,Databricks Configs
# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.scd2source.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.scd2source.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.scd2source.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-22T23:58:46Z&st=2023-07-22T15:58:46Z&spr=https&sig=cc1E9ulir2QVieRhHsen%2B4uRUL%2B81jv6XNYQuITesm0%3D")

# COMMAND ----------

#from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE incr_companies (
# MAGIC      pk_id varchar(255),
# MAGIC      name varchar(255),
# MAGIC      domain varchar(255),
# MAGIC      year_founded varchar(255),
# MAGIC      industry varchar(255),
# MAGIC      size_range varchar(255),
# MAGIC      locality varchar(255),
# MAGIC      country varchar(255),
# MAGIC      linkedin_url varchar(255),
# MAGIC      current_employee_estimate varchar(255),
# MAGIC     total_employee_estimate varchar(255)
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER TABLE Upserts SET TBLPROPERTIES (
# MAGIC --    'delta.columnMapping.mode' = 'name',
# MAGIC --    'delta.minReaderVersion' = '2',
# MAGIC --    'delta.minWriterVersion' = '5')
# MAGIC

# COMMAND ----------

# DBTITLE 1,Transformations/cleaning
c= "companies_sorted.csv"
path='abfss://scd2source@scd2source.dfs.core.windows.net/scd2source/{}'.format(c)
#print(path)
df=spark.read.csv(path,header=True)
#df.show(10)
df1=df.withColumnRenamed('_c0','identity')
df1.printSchema()
display(df1)
#df1.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable('Target')

# COMMAND ----------



# COMMAND ----------

#df1.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable('final_companies')
df1.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable('incr_companies')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE incr_companies SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC --ALTER TABLE incr_companies SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');

# COMMAND ----------

# DBTITLE 1,Merge statement for upsert into delta tables, update+insert=upsert, 
# MAGIC %sql
# MAGIC MERGE into Target as t using 
# MAGIC  (select *, name as pk from upsert_for_scd_2       --These records will be updated/inserted
# MAGIC  union all
# MAGIC  select u.*,null as pk from upsert_for_scd_2 u join Target s on u.name=s.name and s.Is_current='Yes') s  --These records will be inserted
# MAGIC  on t.State=s.pk and t.Is_current='Yes'
# MAGIC  when matched then update set t.End_dt='19-07-2023',t.Is_current='No'
# MAGIC  when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Upserts

# COMMAND ----------

# %sql
# select distinct * from Target
# order by state,Is_current desc;

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit(0)
