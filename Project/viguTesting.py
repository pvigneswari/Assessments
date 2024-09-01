from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

print('This is running from jenkings')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data_Cleaning_Python") \
    .enableHiveSupport() \
    .getOrCreate()

print('This is running from jenkings spark working good')

# Stop the Spark session
spark.stop()
