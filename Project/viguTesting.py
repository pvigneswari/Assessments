from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Incremental_Load") \
    .enableHiveSupport() \
    .getOrCreate()


print('This file is working from jenkins')






# Stop the Spark session
spark.stop()