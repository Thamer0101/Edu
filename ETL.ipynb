#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system(' pip install -U numpy')
get_ipython().system(' pip install missingno')


# In[2]:


import pandas as pd
import matplotlib.pyplot as plt
import os
import configparser
import datetime as dt
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear, avg, monotonically_increasing_id
from pyspark.sql.types import *
import requests
requests.packages.urllib3.disable_warnings()
from pyspark.sql.functions import year, month, dayofmonth, weekofyear, date_format
from pyspark.sql import SparkSession, SQLContext, GroupedData, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql import Row
import datetime, time
import seaborn as sns
import numpy as np
import tools as tools
import tables_func as ct
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# ## Scope of the Project:
# - In this project I will gather the data from two sources. I will load this data into staging dataframes. I will clean the raw data, write it to parquet files and perform an ETL process using a Spark cluster. Then I will write the data into Fact & Dimension tables to form a star schema. The star schema can then be used by the relevant parties to perform data analytics, correlation and ad-hoc reporting in an effective and efficient manner.
# ### Data Descriptions:
# 
# - Edu_ministry is Sample data of saudi scholarships students records from the Ministry of Education. This data source will serve as the Fact table in the schema. This data comes from https://od.data.gov.sa/Data/ar/group/education_and_training?page=2 :'البوابة الوطنية للبيانات المفتوحة'.
# 
# 
# - Countries of the World :This dataset contains Country names linked to region, population, area size, GDP, mortality and more.
# 
#   This data comes from Kaggle: https://www.kaggle.com/datasets/fernandol/countries-of-the-world

# In[3]:


spark = SparkSession    .builder     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")     .getOrCreate()


# In[5]:


#import shutil

#shutil.rmtree('../Project/parquet_tables/countries')


#  ### Spark configuration parameters
#  - ('spark.executor.id', 'driver'),
#  - ('spark.app.name', 'pyspark-shell'),
#  - ('spark.driver.port', '33753'),
#  - ('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0'),
#  - ('spark.rdd.compress', 'True'),
#  - ('spark.serializer.objectStreamReset', '100'),
#  - ('spark.master', 'local[*]'),
#  - ('spark.driver.host', 'ad2cb8c219e1'),
#  - ('spark.submit.deployMode', 'client'),
#  - ('spark.jars',

# In[6]:


fname = '../Project/Sources/Edu_minisrty.csv'
edu_df = pd.read_csv(fname)


# In[7]:


edu_df.head()


# In[8]:


fname = '../Project/Sources/countries of the world.csv'
country_df = pd.read_csv(fname)


# In[9]:


country_df.head()


# ## Exploration and Assessing the Data
# #### Data Cleaning:
# - Drop columns containing over 90% missing values
# - Drop duplicate values

# In[10]:


# checking missing values 
edu_df.isnull().sum()


# In[11]:


# Drop columns with over 90% missing values
clean_edu = tools.eliminate_missing_data(edu_df)


# In[12]:


clean_edu = tools.drop_duplicate_rows(clean_edu)


# In[13]:


# Drop columns with over 90% missing values
clean_country = tools.eliminate_missing_data(country_df)


# In[14]:


clean_country = tools.drop_duplicate_rows(clean_country)


# #### Now I'll translate the dataset to english language

# In[15]:


clean_edu = ct.translate_en(clean_edu)
clean_edu.head()


# In[16]:


clean_edu['Country'].count()


# Males represent No.1 and females No.2

# In[15]:


clean_edu['Sex'].value_counts().plot(kind='pie',autopct='%.2f%%')


# In[17]:


fig, ax = plt.subplots(figsize=(16,4))
clean_edu['Scholarship_type'].value_counts().plot(kind='bar', fontsize =12, rot = 0)
plt.title('Scholarship Types')


# In[18]:


fig, ax = plt.subplots(figsize=(18,6))
clean_edu['Country'].value_counts().plot(kind='bar', fontsize =12, rot = 0)
plt.title('Countries')


# In[19]:


fig, ax = plt.subplots(figsize=(14,6))
clean_edu['Academic_year'].value_counts().plot(kind='bar', fontsize =12, rot = 0)
plt.title('Year')


# In[20]:


fig, ax = plt.subplots(figsize=(12,12))
clean_edu['Specialization'].value_counts().plot(kind='pie',autopct='%.2f%%')


# ## Defining the Data Model
# #### 3.1 Conceptual Data Model
# In accordance with Kimball Dimensional Modelling Techniques, laid out in this document (http://www.kimballgroup.com/wp-content/uploads/2013/08/2013.09-Kimball-Dimensional-Modeling-Techniques11.pdf), the following modelling steps have been taken:
# 
# - Select the Business Process:
#  - The Scholarships department follows their business process of recording scholarships students. This process generates events which are captured and translated to facts in a fact table
# - Declare the Grain:
#  - The grain identifies exactly what is represented in a single fact table row.
#  - In this project, the grain is declared as a single occurrence of a batch of scholarships students out of the KSA.
# - Identify the Dimensions:
#  - Dimension tables provide context around an event or business process.
#  - The dimensions identified in this project are:
#    - dim_country
#    - dim_specialization
# - Identify the Facts:
#  - Fact tables focus on the occurrences of a singular business process, and have a one-to-one relationship with the events described in the grain.
#  - The fact table identified in this project is:
#    - fact_scholarships
#    
# For this application, I have developed a set of Fact and Dimension tables in a Relational Database Management System to form a Star Schema. This Star Schema can be used by Data Analysts and other relevant business professionals to gain deeper insight into various scholarships figures, trends and statistics recorded historically.

# #### 3.2 Mapping Out Data Pipelines
# ##### List the steps necessary to pipeline the data into the chosen data model:
# - 1 Load the data into staging tables
# - 2 Create Dimension tables
# - 3 Create Fact table
# - 4 Write data into parquet files
# - 5 Perform data quality checks
# 

# ## Running Pipelines to Model the Data
# #### 4.1 Create the data model
# Build the data pipelines to create the data model.

# In[17]:


output_path = "parquet_tables/"


# In[18]:


clean_edu.head()


# In[19]:


# creating schema
scholarships_schema = StructType([StructField("Academic_year", StringType(), True)                          ,StructField("Country", StringType(), True)                          ,StructField("Scholarship_type", StringType(), True)                          ,StructField("Educational_level", StringType(), True)                          ,StructField("Specialization", StringType(), True)                          ,StructField("Sex", StringType(), True)                          ,StructField("Count", FloatType(), True)])

scholarships_spark = spark.createDataFrame(clean_edu, schema=scholarships_schema)

scholarships_spark.toPandas().head()


# In[31]:


clean_country.head()


# In[21]:


#"Country","Region","Population","Net migration","Coastline (coast/area ratio)","GDP ($ per capita)
country_schema = StructType([StructField("Country", StringType(), True)                             ,StructField("Region", StringType(), True)                             ,StructField("Population", IntegerType(), True)                             ,StructField("Area (sq. mi.)", IntegerType(), True)                             ,StructField("Pop. Density (per sq. mi.)", StringType(), True)                             ,StructField("Coastline (coast/area ratio", StringType(), True)                             ,StructField("Net migration)", StringType(), True)                             ,StructField("Infant mortality (per 1000 births)", StringType(), True)                             ,StructField("GDP ($ per capita", FloatType(), True)
                            ,StructField("Literacy (%)", StringType(), True)\
                            ,StructField("Phones (per 1000)", StringType(), True)\
                            ,StructField("Arable (%)", StringType(), True)\
                            ,StructField("Crops (%)", StringType(), True)\
                            ,StructField("Other (%)", StringType(), True)\
                            ,StructField("Climate", StringType(), True)\
                            ,StructField("Birthrate", StringType(), True)\
                            ,StructField("Deathrate", StringType(), True)\
                            ,StructField("Agriculture", StringType(), True)\
                            ,StructField("ndustry", StringType(), True)\
                            ,StructField("Service", StringType(), True)])

country_spark = spark.createDataFrame(clean_country, schema=country_schema)

country_spark.toPandas().head()


# In[22]:


# inserting missed rows
added_row = [['Arab Countries', 'MIDLE EAST',0,0,'','','','',0.0,'','','','','','','','','','','',]        ,['Asian Countries', 'ASIA',0,0,'','','','',0.0,'','','','','','','','','','','',]        ,['European Countries', 'EUROPE',0,0,'','','','',0.0,'','','','','','','','','','','',]]
 
# Creating the DataFrame
newRow = spark.createDataFrame(added_row,schema=None)
country_spark= country_spark.union(newRow)


# #### 1. Create dim_Country table

# In[23]:


ct.create_country_dim(country_spark, output_path)


# In[24]:


country = spark.read.parquet("parquet_tables/countries")

country.toPandas().head()


# #### 2. Create dim_Specialization table

# In[25]:


ct.create_specialization_dim(scholarships_spark, output_path)


# In[41]:


specialization = spark.read.parquet("parquet_tables/specializations")

specialization.toPandas().head()


# #### 3. Create fact_Scholarships table

# In[27]:


scholarships = ct.create_scholarships_fact(scholarships_spark, output_path, spark)


# In[28]:


scholarships = spark.read.parquet("parquet_tables/scholarships")

scholarships.toPandas().head()


# In[29]:


scholarships.count()


# ## Data Quality Checks

# In[5]:


tables = {
    "specialization": specialization,
    "country": country,
    "scholarships": scholarships
}

for table_name, table in tables.items():
    tools.perform_quality_check(table, table_name)


# In[6]:


countriesTable = spark.read.parquet("parquet_tables/countries")
specializationTable = spark.read.parquet("parquet_tables/specializations")
scholarshipsTable = spark.read.parquet('parquet_tables/scholarships')


# In[30]:


#Females
scholarshipsTable.filter(scholarshipsTable.Sex == '2')                 .select('scholarships_id', 'Sex')                 .dropDuplicates()                 .count()


# In[31]:


# Males
scholarshipsTable.filter(scholarshipsTable.Sex == '1')                 .select('scholarships_id', 'Sex')                 .dropDuplicates()                 .count()


# In[25]:


query = scholarshipsTable.select(["*"])                .join(countriesTable, (countriesTable.country_id == scholarshipsTable.country_id), how='inner')                .join(specializationTable, (specializationTable.spec_id == scholarshipsTable.spec_id), how='inner')                .select(['scholarships_id' ,specializationTable.spec_desc.alias('Specialization Name'),countriesTable.country_name.alias('Country Name'),countriesTable.Population.alias('Country Population'),                        'Sex', 'Educational_level','Academic_year','count'])                .filter(countriesTable.country_name != ('Asian Countries'))                .filter(countriesTable.country_name != ('European Countries'))                .filter(countriesTable.country_name != ('Arab Countries'))
query.toPandas().head(50)


# In[ ]:




