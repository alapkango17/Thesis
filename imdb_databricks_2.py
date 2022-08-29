# Databricks notebook source
pip install wget

# COMMAND ----------

import wget
import pandas as pd
import gzip
import shutil

# COMMAND ----------

#download the file to local
print('Start the file download')
path ='/tmp/'
url1 = 'https://datasets.imdbws.com/title.principals.tsv.gz'
#url2 = 'https://datasets.imdbws.com/name.basics.tsv.gz'
url3 = 'https://datasets.imdbws.com/title.akas.tsv.gz'
url4 = 'https://datasets.imdbws.com/title.crew.tsv.gz'
url5 = 'https://datasets.imdbws.com/title.episode.tsv.gz'
url6 = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
url7 = 'https://datasets.imdbws.com/title.basics.tsv.gz'
wget.download(url1,path)
#wget.download(url2,path)
wget.download(url3,path)
wget.download(url4,path)
wget.download(url5,path)
wget.download(url6,path)
wget.download(url7,path)

print('Download Complete')

# COMMAND ----------


with gzip.open('/tmp/title.principals.tsv.gz', 'rb') as f_in:
    with open('/tmp/title_principals.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
with gzip.open('/tmp/title.akas.tsv.gz', 'rb') as f_in:
    with open('/tmp/title_akas.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
with gzip.open('/tmp/title.crew.tsv.gz', 'rb') as f_in:
    with open('/tmp/title_crew.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
with gzip.open('/tmp/title.episode.tsv.gz', 'rb') as f_in:
    with open('/tmp/title_episode.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
with gzip.open('/tmp/title.ratings.tsv.gz', 'rb') as f_in:
    with open('/tmp/title_ratings.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
with gzip.open('/tmp/title.basics.tsv.gz', 'rb') as f_in:
    with open('/tmp/title_basics.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)        

# COMMAND ----------

#df1 =pd.read_csv('/tmp/title_principals.tsv','\t')
dbutils.fs.mv("file:/tmp/title_principals.tsv", "dbfs:/FileStore/tables/title_principals.tsv")
#df2 =pd.read_csv('/tmp/title_akas.tsv','\t')
dbutils.fs.mv("file:/tmp/title_akas.tsv", "dbfs:/FileStore/tables/title_akas.tsv")
#df3 =pd.read_csv('/tmp/title_crew.tsv','\t')
dbutils.fs.mv("file:/tmp/title_crew.tsv", "dbfs:/FileStore/tables/title_crew.tsv")
#df4 =pd.read_csv('/tmp/title_episode.tsv','\t')
dbutils.fs.mv("file:/tmp/title_episode.tsv", "dbfs:/FileStore/tables/title_episode.tsv")
dbutils.fs.mv("file:/tmp/title_ratings.tsv", "dbfs:/FileStore/tables/title_ratings.tsv")
#df6 =pd.read_csv('/tmp/title_basics.tsv','\t')
dbutils.fs.mv("file:/tmp/title_basics.tsv", "dbfs:/FileStore/tables/title_basics.tsv")

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/title_ratings',recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/title_akas',recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/title_crew',recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/title_principals',recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/title_episode',recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/title_basics',recurse=True)

# COMMAND ----------

sparkDF1=spark.read.options(header=True, delimiter='\t',inferSchema=True).csv("dbfs:/FileStore/tables/title_principals.tsv")
sparkDF2=spark.read.options(header=True, delimiter='\t',inferSchema=True).csv("dbfs:/FileStore/tables/title_akas.tsv")
sparkDF3=spark.read.options(header=True, delimiter='\t',inferSchema=True).csv("dbfs:/FileStore/tables/title_crew.tsv")
sparkDF4=spark.read.options(header=True, delimiter='\t',inferSchema=True).csv("dbfs:/FileStore/tables/title_episode.tsv")
sparkDF5=spark.read.options(header=True, delimiter='\t',inferSchema=True).csv("dbfs:/FileStore/tables/title_ratings.tsv")
sparkDF6=spark.read.options(header=True, delimiter='\t',inferSchema=True).csv("dbfs:/FileStore/tables/title_basics.tsv")

# COMMAND ----------

sparkDF1.write.saveAsTable("title_principals")
sparkDF2.write.saveAsTable("title_akas")
sparkDF3.write.saveAsTable("title_crew")
sparkDF4.write.saveAsTable("title_episode")
sparkDF5.write.saveAsTable("title_ratings")
sparkDF6.write.saveAsTable("title_basics")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tmp1 as (
# MAGIC SELECT titleType, primaryTitle, startYear, genres, averageRating, numVotes
# MAGIC FROM title_basics as title_basics
# MAGIC INNER JOIN title_ratings as title_ratings ON title_basics.tconst = title_ratings.tconst
# MAGIC WHERE (genres LIKE '%Action%' or genres LIKE '%Drama%'
# MAGIC or genres LIKE '%Comedy%')
# MAGIC AND startYear > 1989 AND averageRating > 7 AND numVotes > 100)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select a.* from title_basics a inner join title_crew b
# MAGIC on a.tconst=b.tconst inner join title_ratings c
# MAGIC on a.tconst=c.tconst inner join title_principals d
# MAGIC on a.tconst=d.tconst order by startyear desc,primarytitle asc
