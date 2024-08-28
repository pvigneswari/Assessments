import os
from pyspark.sql import SparkSession

# Set the Hadoop configuration directory
os.environ['HADOOP_CONF_DIR'] = '/Project/hadoop/config/'


# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFS Connection Example") \
    .getOrCreate()

# Define HDFS file path
hdfs_file_path = "hdfs://ip-172-31-3-80:8020/user/ec2-user/UKUSJULHDFS/enos/raw/aapl/part-m-00000.parquet"


# Read the file into a DataFrame
df = spark.read.text(hdfs_file_path)

# Show the contents of the DataFrame
df.show()

# Stop the Spark session
spark.stop()
