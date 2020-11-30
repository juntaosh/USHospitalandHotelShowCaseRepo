import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import sum, round

complications_filepath = '../../../datasets/hospital/complications_cleaned.csv'
infections_filepath = '../../../datasets/hospital/infections_cleaned.csv'
timely_care_filepath = '../../../datasets/hospital/timely_care_cleaned.csv'
general_ratings_filepath = '../../../datasets/hospital/general_ratings.csv'
general_comparisons_filepath = '../../../datasets/hospital/general_comparisons.csv'

spark = SparkSession \
    .builder \
    .appName("Hospital_Aggregate")\
    .getOrCreate()

# program start here
print("\032[1;31mProgram starts!\033[0m")

complications = spark.read.options(header='True', inferSchema='True') \
        .csv(complications_filepath)
infections = spark.read.options(header='True', inferSchema='True') \
        .csv(infections_filepath)
timely_care = spark.read.options(header='True', inferSchema='True') \
        .csv(timely_care_filepath)
general_ratings = spark.read.options(header='True', inferSchema='True') \
        .csv(general_ratings_filepath)
general_comparisons = spark.read.options(header='True', inferSchema='True') \
        .csv(general_comparisons_filepath)

# find average score of each hospital
avg_inf_cases = infections.groupBy('Facility ID').agg(mean('Observed Cases').alias('Average Number of Infections'))

death_rate_cols = ['Death rate for heart attack patients',
        'Death rate for pneumonia patients',
        'Death rate for COPD patients',
        'Death rate for stroke patients', 
        'Death rate for heart failure patients',
        'Death rate for CABG surgery patients']

complications_cols = ['Broken hip from a fall after surgery',
       'Collapsed lung due to medical treatment',
       'Serious blood clots after surgery',
       'Perioperative Hemorrhage or Hematoma Rate',
       'A wound that splits open after surgery on the abdomen or pelvis',
       'Accidental cuts and tears from medical treatment',
       'Blood stream infection after surgery', 'Pressure sores',
       'Postoperative Acute Kidney Injury Requiring Dialysis Rate',
       'Postoperative Respiratory Failure Rate']

avg_death_rate = complications.where(complications['Measure Name'].isin(death_rate_cols)) \
                .groupBy('Facility ID').agg(mean('Score').alias('Average Death Rate'))

avg_comp_rating = complications.where(complications['Measure Name'].isin(complications_cols)) \
                .groupBy('Facility ID').agg(mean('Score').alias('Average Rating in Complications'))

tc_score_cols = ['Endoscopy/polyp surveillance: colonoscopy interval for patients with a history of adenomatous polyps - avoidance of inappropriate use',
        'Severe Sepsis 3-Hour Bundle',
        'Septic Shock 3-Hour Bundle',
        'Severe Sepsis 6-Hour Bundle',
        'Appropriate care for severe sepsis and septic shock',
        'External Beam Radiotherapy for Bone Metastases',
        'Septic Shock 6-Hour Bundle',
        'Endoscopy/polyp surveillance: appropriate follow-up interval for normal colonoscopy in average risk patients',
        'Healthcare workers given influenza vaccination',
        'Head CT results',
        'Fibrinolytic Therapy Received Within 30 Minutes of ED Arrival',
        "Improvement in Patient's Visual Function within 90 Days Following Cataract Surgery"]

avg_tc_scores = timely_care.where(timely_care['Measure Name'].isin(tc_score_cols)) \
                .groupBy('Facility ID').agg(mean('Score').alias('Average Score in Timely and Effective Care'))
 
overall_score = avg_comp_rating.join(avg_tc_scores, ['Facility ID']).join(avg_death_rate, ['Facility ID']).join(avg_inf_cases, ['Facility ID'])

# find average national comparision of each hospital
# encode comparisons to 1, 0, -1
general_comparisons_cols = [column for column in general_comparisons.columns if 'national' in column]
general_comparisons = general_comparisons.replace('Same as the national average', '0') \
                                .replace('Above the national average', '1') \
                                .replace('Below the national average', '-1')
# change column types
for column in general_comparisons_cols[1:]:
        general_comparisons = general_comparisons.withColumn(column, col(column).cast('integer'))

expression = '`+`'.join(general_comparisons_cols[1:])
# find average comparision of each hospital
general_comparisons = general_comparisons.withColumn('Average National Comparison', expr('(`' + expression + '`)/7'))        

# decode average comparison to text
general_comparisons = general_comparisons.withColumn("Average National Comparison", when(general_comparisons["Average National Comparison"] < 0.0, 'Below National').otherwise(general_comparisons["Average National Comparison"]))
general_comparisons = general_comparisons.withColumn("Average National Comparison", when(general_comparisons["Average National Comparison"] > 0.0, 'Above National').otherwise(general_comparisons["Average National Comparison"]))
general_comparisons = general_comparisons.withColumn("Average National Comparison", when(general_comparisons["Average National Comparison"] == 0.0, 'Same as National').otherwise(general_comparisons["Average National Comparison"]))
general_comparisons = general_comparisons[['Facility ID', 'Average National Comparison']]

# join overall score with national comparison
overall_ratings = general_comparisons.join(overall_score, ['Facility ID']).join(general_ratings, ['Facility ID'])

# write to csv
overall_ratings.coalesce(1).write.option("header","true")\
    .csv("../../../datasets/hospital/overall_ratings.csv")