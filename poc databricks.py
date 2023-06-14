# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.poc11.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.poc11.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.poc11.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-06-15T14:56:43Z&st=2023-06-14T06:56:43Z&spr=https&sig=5uoz1EpESmHVviKnEFGNAsUDp3ftqX5imIX%2Btc7uPkI%3D")

# COMMAND ----------

from pyspark.sql import functions as F
ecomm=spark.read.csv('abfss://carbon@poc11.dfs.core.windows.net/carbo/ecomm_data.csv',header=True)
#ecomm.printSchema()

carbon=spark.read.csv('abfss://carbon@poc11.dfs.core.windows.net/carbo/carbon_emission.csv',header=True)
#carbon.printSchema()

join_df=ecomm.join(carbon,ecomm.Country ==  carbon.Country,"inner").select(ecomm.Quantity,ecomm.CustomerID,ecomm.Country,carbon.Energy_type,carbon.Year,carbon.Population)
#join_df.printSchema()
join_df2=join_df.select(F.col("Quantity").cast('int').alias("Quantity"),F.col("CustomerID"),F.col("Country"),F.col("Energy_type"),F.col("Year"),F.col("Population"))
#join_df2.show(truncate=True)
#join_df2.groupBy(F.col("Year")).agg({"Quantity": "max"}).withColumnRenamed("F.col(SUM(Quantity))", "Quantity").show(truncate=True)
df3_target=join_df2.groupBy("Year")\
  .agg({"Quantity":"min"})\
  .withColumnRenamed("min(Quantity)", "Min_Quantity")


# COMMAND ----------

sfOptions = {
  "sfURL" : "az52918.central-india.azure.snowflakecomputing.com",
  "sfUser" : "venkatakamaiah46",
  "sfPassword" : "Venkatakamaiah46@gmail.com",
  "sfDatabase" : "SFTARGET",
  "sfSchema" : "SFTARGET_SCHEMA",
  "sfWarehouse" : "COMPUTE_WH"
}

# COMMAND ----------

df3_target.rdd.getNumPartitions()

# COMMAND ----------

df3_target.count()

# COMMAND ----------

df3_target.repartition(5).write.format("snowflake").options(**sfOptions).option("dbtable", "SFTARGET_TABLE").mode('overwrite').options(header=True).save()

# COMMAND ----------

#check the data in SFTARGET_TABLE in snowflake
