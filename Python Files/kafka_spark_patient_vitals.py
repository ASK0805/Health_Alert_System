from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# SparkSession 
spark = SparkSession.builder \
    .appName("healthAlertSystem") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "10") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Kafka bootstrap servers
kafka_bootstrap_servers = "34.229.159.47:9092"

# Check if the Kafka server address is correct
assert kafka_bootstrap_servers.count(":") == 1, "Invalid Kafka bootstrap server address"

# Load the JSON data into DataFrame
spark.conf.set("spark.sql.streaming.schemaInference", True)
spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", False)
df1 = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",kafka_bootstrap_servers)
    .option("subscribe", "vital-info")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

df1.printSchema()

# Convert 'value' column to string
df2 = df1.withColumn("value", expr("cast(value as string)"))
df2.printSchema()

# Define the schema for JSON data
json_schema = StructType([
    StructField("customerId", StringType(), True),
    StructField("heartBeat", StringType(), True),
    StructField("bp", StringType(), True)
])

# Parse JSON data and select fields
streaming_df = df2.withColumn("final_value", from_json(col("value"), json_schema)).selectExpr("final_value.*", "timestamp")
df_final = streaming_df.withColumn('date',lit('2024-07-21')) 
df_final.printSchema()

# Write the streaming DataFrame to the console
query = df_final.select("customerId", "heartBeat", "bp", "timestamp", "date") \
    .writeStream \
    .format("parquet") \
    .partitionBy("date") \
    .outputMode("append") \
    .option("path","/home/hadoop/data1/vital.parquet") \
    .option("checkpointLocation", "/checkpoint_dir_00") \
    .trigger(processingTime="1 seconds") \
    .start()

query.awaitTermination()
