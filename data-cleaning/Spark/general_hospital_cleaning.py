"""
Cleans Hospital_General_Information.csv

Execution: 
    python general_hospital_cleaning.py
Output: 
    1) hospital_info.csv - ready to be imported into MySQL
    2) general_ratings.csv - ready to be imported into MySQL
    3) general_comparisons.csv - ready to be imported into MySQL
    4) hospital_list.csv - contains Facility ID & Name. Used in cleaning.
"""

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# get data from the hospital file
filepath = '../../../datasets/Hospital_General_Information.csv'
spark = SparkSession \
    .builder \
    .appName("General_Hospital_Information")\
    .getOrCreate()

# program start here
print("\032[1;31mProgram starts!\033[0m")

df = spark.read.options(header = 'True', inferSchema = 'True')\
    .csv(filepath)

# drop unnecessary columns
columns_to_drop = ["Hospital Ownership",
                   "Hospital overall rating footnote",
                   "Mortality national comparison footnote",
                   "Safety of care national comparison footnote",
                   "Readmission national comparison footnote",
                   "Patient experience national comparison footnote",
                   "Effectiveness of care national comparison footnote",
                   "Timeliness of care national comparison footnote",
                   "Efficient use of medical imaging national comparison footnote"]

df2 = df.drop(*columns_to_drop)

# drop rows with duplicated Facility ID's
df2 = df2.dropDuplicates(subset=['Facility ID'])

# change data types
df2 = df2.withColumn("Facility ID", round(col("Facility ID")).cast("Integer"))\
    .withColumn("Facility ID", col("Facility ID").cast("String"))\
    .withColumn("ZIP Code", col("ZIP Code").cast("String"))\
    .withColumn("Hospital overall rating", col("Hospital overall rating").cast("Integer"))

# replace values with null
df2 = df2.replace('Not Applicable', None)\
    .replace('Not Available', None)\
    .replace('nan', None)

# drop rows with null values for Facility ID or Name
df2 = df2.dropna(how='any')
# change location format to lat, long
df2 = df2.withColumn("Location", regexp_extract(col("Location"), "(\W\d+.\d+\W+\d+.\d+)", 1))
split_location = split(df2['Location'], ' ')
df2 = df2.withColumn("Long", split_location[0]).withColumn('Lat', split_location[1])
df2 = df2.drop("Location")
# df2.show(5)

# separate hospital info from general ratings
info_cols = ['Address', 'City', 'State', 'ZIP Code','County Name', 'Phone Number', 'Long', 'Lat', 'Hospital Type', 'Emergency Services']
rating_cols = ['Meets criteria for promoting interoperability of EHRs', 'Hospital overall rating']
hospital_info = df2[['Facility ID', 'Facility Name'] + info_cols]
general_ratings = df2[['Facility ID'] + rating_cols]
general_comparisons = df2.drop(*info_cols).drop(*rating_cols).drop('Facility Name')
hospital_list = df2['Facility ID', 'Facility Name']

# write to csv
hospital_info.coalesce(1).write.option("header","true")\
    .csv("../../../datasets/hospital/hospital_info.csv")
general_ratings.coalesce(1).write.option("header","true")\
    .csv("../../../datasets/hospital/general_ratings.csv")
general_comparisons.coalesce(1).write.option("header","true")\
    .csv("../../../datasets/hospital/general_comparisons.csv")
hospital_list.coalesce(1).write.option("header","true")\
    .csv("../../../datasets/hospital/hospital_list.csv")