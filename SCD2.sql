-- Databricks notebook source
-- DBTITLE 1,MERGING TABLES FOR GETTING UPDATES AND INSERTS (SCD_2)
-- MAGIC %md MERGING TABLES

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC truncate table old_and_target_for_scd_2;
-- MAGIC insert into old_and_target_for_scd_2(State,Capital,Start_dt,End_dt,Is_current) 
-- MAGIC select * from old_and_target_for_scd_2 VERSION AS OF 0;
-- MAGIC

-- COMMAND ----------

select * from old_and_target_for_scd_2

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from default.upsert_for_scd_2

-- COMMAND ----------

-- MAGIC %md Do not Rerun Merge Stmt as Target table will merged multiple times   
-- MAGIC

-- COMMAND ----------

MERGE into old_and_target_for_scd_2 as t using 
(select *,State as pk from upsert_for_scd_2       --These records will be updated/inserted
union all
select u.*,null as pk from upsert_for_scd_2 u join old_and_target_for_scd_2 s on u.State=s.State and s.Is_current='Yes') s  --These records will be inserted
on t.State=s.pk and t.Is_current='Yes'
when matched then update set t.End_dt='19-07-2023',t.Is_current='No'
when not matched then insert *

-- COMMAND ----------

select distinct * from old_and_target_for_scd_2
order by state,Is_current desc;
