# Import Libraries
import pandas as pd
from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import *

import plotly.plotly as py
import plotly.graph_objs as go
import requests
requests.packages.urllib3.disable_warnings()

from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth, weekofyear, date_format

from pyspark.sql import SparkSession, SQLContext, GroupedData, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql import Row

import datetime, time

import tools as tools



def create_country_dim(input_df, output_data):
    """
        Gather country data, create dataframe and write data into parquet files.
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing country dimension
    """
    
    df = input_df.withColumn("country_id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1) \
                .select(["country_id", "Country","Region","Population","Coastline (coast/area ratio","Net migration)","GDP ($ per capita"]) \
                .withColumnRenamed("Country", "country_name")\
                .withColumnRenamed("Coastline (coast/area ratio", "Coastline_area_ratio")\
                .withColumnRenamed("Net migration)", "Net_migration")\
                .withColumnRenamed("GDP ($ per capita", "GDP_$_per_capital")\
    
    tools.write_to_parquet(df, output_data, "countries")
    
    return df


def create_specialization_dim(input_df, output_data):
    """
        Gather specialization data, create dataframe and write data into parquet files.
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing specialization dimension
    """
    
    df = input_df.dropDuplicates(["Specialization"])\
                 .withColumn("spec_id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1) \
                 .select(["spec_id", "Specialization"]) \
                .withColumnRenamed("Specialization", "spec_desc")
    
    tools.write_to_parquet(df, output_data, "specializations")
    
    return df


def create_scholarships_fact(scholarships_spark, output_data, spark):
    """
        Gather scholarships data, create dataframe and write data into parquet files.
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing immigration fact
    """
    
    countries = spark.read.parquet("parquet_tables/countries")
    specialization = spark.read.parquet("parquet_tables/specializations")

    # join all tables to scholarships
    df = scholarships_spark.select(["*"])\
                .join(countries, (scholarships_spark.Country == countries.country_name), how='inner')\
                .join(specialization, (scholarships_spark.Specialization == specialization.spec_desc), how='inner')\
                .withColumn("scholarships_id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1) \
                .select(["scholarships_id", specialization.spec_id, countries.country_id, "Academic_year", "Educational_level", "Sex", "Count"])
    
    tools.write_to_parquet(df, output_data, "scholarships")
    
    return 


def translate_en(df):
        df.rename(columns = {'السنة الدراسية':'Academic_year', 'القطاع الدولي':'Country', 'فئة الإبتعاث':'Scholarship_type',
                               'المرحلة الدراسية ':'Educational_level','المجال الواسع':'Specialization','الجنس':'Sex','العدد':'Count'},inplace = True)                                    
        df.replace({'أخرى':'Other','ذكر' :'1','أنثى' :'2', 'طالب مبتعث':'Scholarship student', 'موظف مبتعث':'Expatriate employee', 'دارس على حسابه الخاص':'on his own',
                    
                      'الدول العربية':'Arab Countries ','كندا':'Canada ','أمريكا':'United States ','بريطانيا':'United Kingdom',
                    'الدول الأسيوية':'Asian Countries','الدول الأوروبية الأخرى':'European Countries',
                   'استراليا':'Australia',
        # Education Level                   
        'الزمالة':'Fellowship','دبلوم عالي':'High diploma','دكتوراه':'Ph.d','ماجستير':'Master','دبلوم':'Diploma','بكالوريوس':'Bachelor','زمالة':'Fellowship',
        # Specializations
        'غير مصنفة':'Not Specified','الفنون والعلوم الإنسانية':'Arts and humanities','العلوم الاجتماعية، والصحافة والإعلام':'Social Sciences, Journalism and Media','الخدمات':'Services',
        'البرامج و المؤهلات غير المتخصصة':'Non-specialized programs and qualifications',
        'تكنولوجيا الاتصالات والمعلومات':'Communication and IT','تكنولوجيا الاتصالات، والمعلومات':'Communication_IT'
        ,'الفنون':'Art ','اللغات':'Languages ','البرامج والمؤهلات الأساسية':'Law ','القانون':'Law ','العلوم الفيزيائية':'Physics','الصحة':'Health ','الأعمال والإدارة':'Businesses and Management','الهندسة والحرف الهندسية':'Engineering and engineering crafts'
,'الهندسة، والتصنيع والبناء':'Engineering, manufacturing and construction ','العلوم الطبيعية، والرياضايات والإحصاء':'Natural sciences, mathematics and statistics','الصحة والرفاه':'Health and well-being ','التعليم':'Education','الأعمال، الإدارة والقانون':'Business, Management and Law'
,'العلوم الاجتماعية السلوكية':'Behavioral Social Sciences ','العلوم البيولوجية والعلوم المتصلة بها':'Biological and related sciences ','الزراعة':'Farming ','الرفاه':'well-being ','الدراسات الإنسانية':'Human studies ','الزراعة، والحراجة، ومصائد الأسماك والبيطرة':'Agriculture, forestry, fisheries and veterinary '
,'أخرى في الهندسة والتصنيع والبناء':'Others in engineering, manufacturing and construction','الخدمات الشخصية':'Personal services','خدمات الأمن':'Security services','البيئة':'Environment','خدمات النظافة العامة والصحة المهنية':'General hygiene and occupational health services','الهندسة المعمارية والبناء':'Architecture and construction','خدمات النقل':'Transport services','الرياضيات والإحصاء':'Mathematics and statistics','التصنيع والمعالجة':'Manufacturing and processing','الصحافة والإعلام':'Press and media',
          'برامج رئيسة في العلوم الطبيعية، والرياضيات والإحصاء':'Major programs in natural sciences, mathematics and statistics','أخرى في الزراعة، والحراجة، ومصايد الأسماك والبيطرة':'Others in agriculture, forestry, fisheries and veterinary','غير محدد':'Undefined',
                    },inplace = True)
        return df
