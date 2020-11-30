"""
Cleans Healthcare_Associated_Infections_-_Hospital.csv

Execution: 
    python infections_cleaning.py
Output: 
    1) infections_cleaned.csv - ready to be imported into MySQL
"""

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# get data from the hospital file
infections_filepath = '../../../datasets/hospital/Healthcare_Associated_Infections_-_Hospital.csv'
ids_filepath = '../../../datasets/hospital/hospital_list.csv'
spark = SparkSession \
    .builder \
    .appName("Infections")\
    .getOrCreate()

# program start here
print("\032[1;31mProgram starts!\033[0m")

df = spark.read.options(header='True', inferSchema='True')\
    .csv(infections_filepath)
ids = spark.read.options(header='True', inferSchema='True')\
    .csv(ids_filepath)

# drop uneccessary columns
columns_to_drop = ['Facility Name', 'Address', 'City', 'State', 'ZIP Code','County Name', 'Phone Number', 'Location', "Measure ID",
                    "Footnote", "Measure Start Date", "Measure End Date"]
df2 = df.drop(*columns_to_drop)

# drop rows that are not observed cases
measure_keep = ['Catheter Associated Urinary Tract Infections (ICU + select Wards): Observed Cases',
                'SSI - Colon Surgery: Observed Cases',
                'Clostridium Difficile (C.Diff): Observed Cases',
                'MRSA Bacteremia: Observed Cases',
                'Central Line Associated Bloodstream Infection (ICU + select Wards): Observed Cases',
                'SSI - Abdominal Hysterectomy: Observed Cases']
df2 = df2.where(df2['Measure Name'].isin(measure_keep))

# change datatypes
df2 = df2.withColumn("Facility ID", round(col("Facility ID")).cast("Integer"))\
    .withColumn("Facility ID", col("Facility ID").cast("String"))\
    .withColumn("Score", col("Score").cast("Float"))

# drop rows with duplicated Facility ID's
df2 = df2.dropDuplicates(subset=['Facility ID', 'Measure Name'])

# replace values with null
df2 = df2.replace('Not Applicable', None)\
    .replace('Not Available', None)\
    .replace('nan', None)

# drop rows with null values
df2 = df2.dropna(how='any')

# natural join with facility name
df2 = df2.join(ids, df2['Facility ID'] == ids['Facility ID'])\
    .drop(ids['Facility Name'])\
    .drop(df2['Facility ID'])

# rearrange columns so that faculty id and name are at the left
column_names = df2.schema.names
column_names = column_names[-1:] + column_names[:-1]
df2 = df2.select(column_names).withColumnRenamed("Score", "Observed Cases")

# write to csv
df2.coalesce(1).write.option("header","true")\
    .csv("../../../datasets/hospital/infections_cleaned.csv")