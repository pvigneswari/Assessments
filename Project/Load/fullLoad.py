from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data_Cleaning_Python") \
    .enableHiveSupport() \
    .getOrCreate()

# Check if the correct number of arguments are provided
if len(sys.argv) != 9:
    print("Usage: script.py <patient_csv> <hospital_treatment_csv> <insurance_csv> <medical_cost_csv> <output_patient_csv> <output_hospital_treatment_csv> <output_insurance_csv> <output_medical_cost_csv>")
    sys.exit(1)

# Assign command-line arguments to variables
patient_csv = sys.argv[1]
hospital_treatment_csv = sys.argv[2]
insurance_csv = sys.argv[3]
medical_cost_csv = sys.argv[4]
output_patient_csv = sys.argv[5]
output_hospital_treatment_csv = sys.argv[6]
output_insurance_csv = sys.argv[7]
output_medical_cost_csv = sys.argv[8]

# Define schemas based on the provided files
patient_schema = StructType([
    StructField("Patient_id", StringType()),
    StructField("Name", StringType()),
    StructField("Address", StringType()),
    StructField("Phone", StringType()),
    StructField("Email", StringType())
])

hospital_treatment_schema = StructType([
    StructField("Patient_id", StringType()),
    StructField("treatment", StringType()),
    StructField("treatment_cost", FloatType()),
    StructField("hospital", StringType()),
    StructField("date_of_treatment", StringType())
])

insurance_schema = StructType([
    StructField("Patient_id", StringType()),
    StructField("insurance_company", StringType()),
    StructField("policy_number", StringType()),
    StructField("coverage_amount", FloatType()),
    StructField("insurance_cost", FloatType())
])

medical_cost_schema = StructType([
    StructField("age", IntegerType()),
    StructField("sex", StringType()),
    StructField("bmi", FloatType()),
    StructField("smoker", StringType()),
    StructField("region", StringType()),
    StructField("medical_cost", FloatType()),
    StructField("Patient_id", StringType())
])

# Load the datasets
patients_df = spark.read.option("header", "true").schema(patient_schema).csv(patient_csv)
hospital_treatment_df = spark.read.option("header", "true").schema(hospital_treatment_schema).csv(hospital_treatment_csv)
insurance_df = spark.read.option("header", "true").schema(insurance_schema).csv(insurance_csv)
medical_cost_df = spark.read.option("header", "true").schema(medical_cost_schema).csv(medical_cost_csv)

# Remove duplicates
patients_df = patients_df.dropDuplicates()
hospital_treatment_df = hospital_treatment_df.dropDuplicates()
insurance_df = insurance_df.dropDuplicates()
medical_cost_df = medical_cost_df.dropDuplicates()

# Database connection parameters
host = "ec2-18-132-73-146.eu-west-2.compute.amazonaws.com"  # Your PostgreSQL server address
port = "5432"                                               # Your PostgreSQL port
dbname = "testdb"                                           # Your database name
user = "consultants"                                        # Your PostgreSQL username
password = "WelcomeItc@2022"                                # Your PostgreSQL password

# Construct JDBC URL
postgres_url = "jdbc:postgresql://{0}:{1}/{2}".format(host, port, dbname)

# JDBC properties
postgres_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Save cleaned data to PostgreSQL
patients_df.write.jdbc(url=postgres_url, table="patients_table", mode="overwrite", properties=postgres_properties)
hospital_treatment_df.write.jdbc(url=postgres_url, table="hospital_treatment_table", mode="overwrite", properties=postgres_properties)
insurance_df.write.jdbc(url=postgres_url, table="insurance_table", mode="overwrite", properties=postgres_properties)
medical_cost_df.write.jdbc(url=postgres_url, table="medical_cost_table", mode="overwrite", properties=postgres_properties)

# Save cleaned data to Hive tables
patients_df.write.mode("overwrite").saveAsTable("ukusjul.patients_table")
hospital_treatment_df.write.mode("overwrite").saveAsTable("ukusjul.hospital_treatment_table")
insurance_df.write.mode("overwrite").saveAsTable("ukusjul.insurance_table")
medical_cost_df.write.mode("overwrite").saveAsTable("ukusjul.medical_cost_table")

# Save cleaned data as CSV to provided output paths
patients_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_patient_csv)
hospital_treatment_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_hospital_treatment_csv)
insurance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_insurance_csv)
medical_cost_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_medical_cost_csv)
patients_df.show()

# Stop the Spark session
spark.stop()
