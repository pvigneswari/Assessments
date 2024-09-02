from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.types import TimestampType

# Create SparkSession
spark = SparkSession.builder \
    .appName("Spark_Streaming") \
    .master("local[1]") \
    .getOrCreate()

# Define schema for the streaming data
schema = StructType([
    StructField("Patient_ID", IntegerType()),
    StructField("treatment_cost", DoubleType()),
    StructField("Treatment_Status", StringType()),
    StructField("timestamp", TimestampType(), True),

])

# Define Kafka bootstrap servers
bootstrap_servers = "ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

# Set Hive metastore URI
spark.conf.set("hive.metastore.uris", "thrift://ec2-18-132-63-52.eu-west-2.compute.amazonaws.com:9083")

# Read data from Kafka
streamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "trans") \
    .option("startingOffsets", "latest") \
    .load()

# Split the received data into columns
splitCols = f.split(f.col("value"), ",")

# Create DataFrame with schema
formattedDF = streamingDF \
    .select(
        splitCols.getItem(0).cast(IntegerType()).alias("Patient_ID"),
        splitCols.getItem(1).cast(DoubleType()).alias("treatment_cost"),
        splitCols.getItem(2).cast(StringType()).alias("Treatment_Status"),
        splitCols.getItem(3).cast(TimestampType()).alias("timestamp")
    )

# Filter the streaming DataFrame to include only "failed" transactions and add watermarking
failed_transactions_df = formattedDF \
    .filter(f.col("Treatment_Status") == "Unprocessed") \
    .withWatermark("timestamp", "10 minutes")  # Add watermarking

# Perform real-time analytics (e.g., display count of failed transactions)

# Write the streaming output to Parquet
query = failed_transactions_df \
     .writeStream \
     .format("parquet") \
     .option("path", "/user/ec2-user/UKUSJULHDFS/vigu/newkafka") \
     .option("checkpointLocation", "/user/ec2-user/UKUSJULHDFS/vigu/newoffset") \
     .outputMode("append") \
     .start()

# Wait for the termination of the query
query.awaitTermination()
