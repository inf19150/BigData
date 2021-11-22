#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark, os
findspark.init("/home/hadoop/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import numpy as np


spark=SparkSession.builder.master("yarn").appName("Jupyter").enableHiveSupport().getOrCreate()


# In[ ]:


df_test=spark.read.format("csv").options(header="true", delimiter=",", nullValue="null", inferSchema="true").load("/user/hadoop/ocid/work/*.csv")


# In[ ]:


df_test.printSchema()


# In[ ]:


df_test.repartition('radio').write.format("parquet").mode("overwrite").partitionBy("radio").save("/user/hadoop/ocid/work/final")


# In[ ]:


df_test.repartition('radio').write.format("parquet") .mode("overwrite").option("path", "/user/hadoop/ocid/work/final_table").partitionBy("radio") .saveAsTable("default.partitioned_radio") 


# In[ ]:


df_test.where(col("lat") > 47).where(col("lon") > 6).where(col("lat") < 55).where(col("lon") < 15).groupBy(col("radio")).agg(round(avg("range"),3)).show(5)


# In[16]:


# Read table from csv-file
df_full=spark.read.format("csv").options(header="true", delimiter=",", nullValue="null", inferSchema="true").load("/user/hadoop/ocid/work/cell_towers.csv")
df_full.printSchema()
df_full.count()


# In[ ]:





# In[ ]:





# In[100]:


#spark.sql("DROP TABLE IF EXISTS default.lat_lon_range")
spark.sql("DROP TABLE IF EXISTS default.test_lat_lon_range")


# In[101]:


spark.sql("SHOW TABLES").show(10, False)


# In[21]:


spark.table("default.lat_lon_range").where(col("radio") == "LTE").show(10)


# In[18]:


#filter&repartition by radio-type and save as table start: 15:03-15:08
df_full.select("radio", "lat","lon","range").repartition('radio').write.format("parquet").mode("overwrite").option("path", "/user/hadoop/ocid/work/final").partitionBy("radio").saveAsTable("default.lat_lon_range")


# In[22]:


#df_lte_only=spark.read.format("parquet").options(header="true", delimiter=",", nullValue="null", inferSchema="true").load("/user/hadoop/ocid/work/final/radio=LTE")
df_lte_only=spark.table("default.lat_lon_range").where(col("radio") == "LTE")
df_umts_only=spark.table("default.lat_lon_range").where(col("radio") == "UMTS")
df_gsm_only=spark.table("default.lat_lon_range").where(col("radio") == "GSM")


# In[23]:


df_lte_only.show(15)


# In[24]:


lat=48.741959


# In[25]:


lon=8.677007


# In[91]:


#raw_local_records=df_lte_only.where(col("lat") > lat-0.01).where(col("lon") > lon -0.01).where(col("lat") < lat+0.01).where(col("lon") < lon+0.01)
def getCoverageatGrid(table_str, lat, lon):
    df_result = spark.table(table_str)     .where(col("lat") > lat-0.01).where(col("lon") > lon -0.01)     .where(col("lat") < lat+0.01).where(col("lon") < lon+0.01)     .groupBy("radio").agg(avg("range")).sort(asc("radio"))
    return dict(df_result.collect())


# In[75]:


getCoverageatGrid(lat, lon)


# In[81]:


coverage_dict=dict()
def extractGermany():
    #STEP=0.01
    STEP=1
    lat_range=np.arange(46.959149, 55.019144, STEP)
    lon_range=np.arange(5.704355, 15.186580, STEP)
    for lat in lat_range:
        for lon in lon_range:
            print("Fetching " + str(lat), str(lon) + "... ", end="")
            coverage_dict[(lat, lon)] = getCoverageatGrid("default.lat_lon_range", lat, lon)
            print("Done")
extract=extractGermany()    


# In[93]:


coverage_dict


# In[ ]:


#Same procedure for daily-dag...


# In[84]:


# Read daily-table from csv-file
df_diff=spark.read.format("csv").options(header="true", delimiter=",", nullValue="null", inferSchema="true").load("/user/hadoop/ocid/work/ocid_diff_2021-11-15.csv")
df_diff.printSchema()
df_diff.count()


# In[ ]:





# In[96]:


#filter&repartition by radio-type and append to table
df_diff.select("radio", "lat","lon","range").repartition('radio').write.format("parquet").mode("append").option("path", "/user/hadoop/ocid/work/final").partitionBy("radio").saveAsTable("default.lat_lon_range")


# In[98]:


spark.table("default.lat_lon_range").count()


# In[103]:


spark.table("default.lat_lon_range").write.format('json').save("/user/hadoop/ocid/dump.json")


# In[ ]:


df_diff_lte_only=spark.table("default.test_lat_lon_range").where(col("radio") == "LTE")
df_diff_umts_only=spark.table("default.test_lat_lon_range").where(col("radio") == "UMTS")
df_diff_gsm_only=spark.table("default.test_lat_lon_range").where(col("radio") == "GSM")


# In[92]:


coverage_dict_test=dict()
def extractGermany():
    #STEP=0.01
    STEP=1
    lat_range=np.arange(46.959149, 55.019144, STEP)
    lon_range=np.arange(5.704355, 15.186580, STEP)
    for lat in lat_range:
        for lon in lon_range:
            print("Fetching " + str(lat), str(lon) + "... ", end="")
            coverage_dict_test[(lat, lon)] = getCoverageatGrid("default.test_lat_lon_range", lat, lon)
            print("Done")
extract=extractGermany()  


# In[94]:


coverage_dict_test


# In[ ]:




