![shco](https://user-images.githubusercontent.com/127024138/223195693-8310b14b-9054-42fd-928d-58b47ca9cb6a.PNG)
# Data-Engineering-Project
This project aims to combine two data sets containing shcolarships data and Countries data. The primary purpose of the combination is to create a schema which can be used to derive various correlations, trends and analytics. 
_________________________________________________________________________________________________________________________________________________________________________________________
## Datasets:
- Edu_ministry dataset is a sample data of saudi scholarships students records since 2015 to 2018 provided from the Ministry of Education.. This data source will serve as the Fact table in the schema.
  - This data comes from https://od.data.gov.sa/Data/ar/group/education_and_training?page=2 :'البوابة الوطنية للبيانات المفتوحة'.
- Countries dataset of the World :This dataset contains Country names linked to region, population, area size, GDP, mortality and more.
  - This data comes from Kaggle: https://www.kaggle.com/datasets/fernandol/countries-of-the-world

# Data Model:
_________________________________________________________________________________________________________________________________________________________________________________________

In accordance with Kimball Dimensional Modelling Techniques, laid out in this document (http://www.kimballgroup.com/wp-content/uploads/2013/08/2013.09-Kimball-Dimensional-Modeling-Techniques11.pdf), the following modelling steps have been taken:

- Select the Business Process:
 - The Scholarships department follows their business process of recording scholarships students. This process generates events which are captured and translated to facts in a fact table
- Declare the Grain:
 - The grain identifies exactly what is represented in a single fact table row.
 - In this project, the grain is declared as a single occurrence of a batch of scholarships students out of the KSA.
- Identify the Dimensions:
 - Dimension tables provide context around an event or business process.
 - The dimensions identified in this project are:
   - dim_country
   - dim_specialization
- Identify the Facts:
 - Fact tables focus on the occurrences of a singular business process, and have a one-to-one relationship with the events described in the grain.
 - The fact table identified in this project is:
   - fact_scholarships
   
For this application, I have developed a set of Fact and Dimension tables in a Relational Database Management System to form a Star Schema. This Star Schema can be used by Data Analysts and other relevant business professionals to gain deeper insight into various scholarships figures, trends and statistics recorded historically.


#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model:
 - Load the data into staging tables
 - Create Dimension tables
 - Create Fact table
 - Write data into parquet files
 - Perform data quality checks
# Project Write Up:
### Tools and technologies used in this project:
 This project makes use of various Big Data processing technologies including:
  - Apache Spark, because of its ability to process massive amounts of data as well as the use of its unified analytics engine and convenient APIs
  - Pandas, due to its convenient dataframe manipulation functions
  - Matplotlib, to plot data and gain further insights
### Other scenarios:
 1. The data was increased by 100x:
   - If the data was increased by 100x I would use more sophisticated and appropriate frameworks to perform processing and storage functions, such as Amazon Redshift, Amazon EMR or Apache Cassandra.
 2. The data populates a dashboard that must be updated on a daily basis by 7am every day:
   - If the data had to populate a dashboard daily, I would manage the ETL pipeline in a DAG from Apache Airflow. This would ensure that the pipeline runs in time, that data quality checks pass, and provide a convenient means of notification should the pipeline fail.
 3. The database needed to be accessed by 100+ people:
   - If the data needed to be accessed by many people simultaneously, I would move the analytics database to Amazon Redshift which can handle massive request volumes and is easily scalable.
