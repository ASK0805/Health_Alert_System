from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("healthAlertSystem") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "10") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Check if the table exists
tables = spark.sql("SHOW TABLES IN patient_health_data").filter("tableName = 'patient_contact_info'").collect()
if len(tables) == 0:
    raise Exception("Table patient_health_data.patient_contact_info does not exist")

# Read the patient contact information table
patient_contact_df = spark.table("patient_health_data.patient_contact_info")

# Define the schema for the streaming data
schema = StructType([
    StructField('CustomerID', IntegerType(), True),
    StructField('BP', IntegerType(), True),
    StructField('HeartBeat', IntegerType(), True),
    StructField('Message_time', TimestampType(), True)
])

# Read patient vitals streaming data from an HDFS location
patient_vital_df = spark.readStream.format("parquet") \
    .schema(schema) \
    .load("/home/hadoop/data1/vital.parquet/date=2024-07-21")

# Creating a temporary table for streaming data
patient_vital_df.createOrReplaceTempView("Patients_Vital_Info")

# Selecting the patient details with abnormal vitals
alert_df = spark.sql("""
SELECT 
    patientname, age, patientaddress, phone_number, admitted_ward, 
    BP as bp, HeartBeat as heartBeat, Message_time as input_Message_time, alert_message 
FROM 
    Patients_Vital_Info v
JOIN 
    patient_health_data.Threshold_Reference_Table T 
ON 
    (C.age BETWEEN T.low_age_limit AND T.high_age_limit)
JOIN 
    patient_health_data.Patient_Contact_Info C 
ON 
    (C.patientid = v.CustomerID)
WHERE 
    ((T.attribute = 'bp' AND (v.BP BETWEEN T.low_range_value AND T.high_range_value)) 
    OR (T.attribute = 'heartBeat' AND (v.HeartBeat BETWEEN T.low_range_value AND T.high_range_value))) 
    AND T.alert_flag = 1
""")

# Generating a new column with current timestamp
alert_df = alert_df.withColumn("alert_generated_time", current_timestamp())

# Write streaming health alerts data to Alerts_Message kafka topic
alert_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "34.229.159.47:9092") \
    .option("topic", "Alerts_Messages") \
    .option("checkpointLocation", "/tmp/alert_checkpoint/") \
    .start() \
    .awaitTermination()
