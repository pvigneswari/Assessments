from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Incremental_Load") \
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
    StructField("children", StringType()),
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

patients_df.show(10)
patients_df.printSchema()
print("Number of rows in patients_df:  {}".format(patients_df.count()))
hospital_treatment_df.show(10)
hospital_treatment_df.printSchema()
print("Number of rows in hospital_treatment_df: {}".format(hospital_treatment_df.count()))
insurance_df.show(10)
insurance_df.printSchema()
print("Number of rows in insurance_df: {}".format(insurance_df.count()))
medical_cost_df.show(10)
medical_cost_df.printSchema()
print("Number of rows in medical_cost_df: {}".format(medical_cost_df.count()))


# Remove duplicates
# patients_df = patients_df.dropDuplicates()
# hospital_treatment_df = hospital_treatment_df.dropDuplicates()
# insurance_df = insurance_df.dropDuplicates()
# medical_cost_df = medical_cost_df.dropDuplicates()

# Perform incremental loading by using a condition or filter on the data
# Example: Filter for only the latest records based on a date or unique identifier

# Here, we simulate filtering logic; replace it with actual conditions as needed
incremental_patients_df = patients_df  # Replace with filtering logic if needed
incremental_hospital_treatment_df = hospital_treatment_df  # Example filter
incremental_insurance_df = insurance_df  # Replace with filtering logic if needed
incremental_medical_cost_df = medical_cost_df  # Replace with filtering logic if needed

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

# Load existing data from PostgreSQL (if required for comparison or merging)
existing_patients_df = spark.read.jdbc(url=postgres_url, table="patients_table", properties=postgres_properties)
existing_hospital_treatment_df = spark.read.jdbc(url=postgres_url, table="hospital_treatment_table", properties=postgres_properties)
existing_insurance_df = spark.read.jdbc(url=postgres_url, table="insurance_table", properties=postgres_properties)
existing_medical_cost_df = spark.read.jdbc(url=postgres_url, table="medical_cost_table", properties=postgres_properties)

# Logic for merging or updating with new data goes here
# For example, using union, join, or other DataFrame operations

# Save incremental data to PostgreSQL
incremental_patients_df.write.jdbc(url=postgres_url, table="patients_table", mode="append", properties=postgres_properties)
incremental_hospital_treatment_df.write.jdbc(url=postgres_url, table="hospital_treatment_table", mode="append", properties=postgres_properties)
incremental_insurance_df.write.jdbc(url=postgres_url, table="insurance_table", mode="append", properties=postgres_properties)
incremental_medical_cost_df.write.jdbc(url=postgres_url, table="medical_cost_table", mode="append", properties=postgres_properties)
incremental_patients_df.show(10)
incremental_patients_df.printSchema()
print("Number of rows in incremental_patients_df:  {}".format(incremental_patients_df.count()))
incremental_hospital_treatment_df.show(10)
incremental_hospital_treatment_df.printSchema()
print("Number of rows in incremental_hospital_treatment_df:  {}".format(incremental_hospital_treatment_df.count()))
incremental_insurance_df.show(10)
incremental_insurance_df.printSchema()
print("Number of rows in incremental_insurance_df:  {}".format(incremental_insurance_df.count()))
incremental_medical_cost_df.show()
incremental_medical_cost_df.printSchema()
print("Number of rows in incremental_medical_cost_df:  {}".format(incremental_medical_cost_df.count()))

# Save only top 10 records to Hive tables
incremental_patients_df.limit(10).write.saveAsTable("julbatch.patients_table")
incremental_hospital_treatment_df.limit(10).write.mode("overwrite").saveAsTable("julbatch.hospital_treatment_table")
incremental_insurance_df.limit(10).write.mode("overwrite").saveAsTable("julbatch.insurance_table")
incremental_medical_cost_df.limit(10).write.mode("overwrite").saveAsTable("julbatch.medical_cost_table")

# Save cleaned data as CSV to provided output paths
incremental_patients_df.coalesce(1).write.option("header", "true").csv(output_patient_csv)
incremental_hospital_treatment_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_hospital_treatment_csv)
incremental_insurance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_insurance_csv)
incremental_medical_cost_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_medical_cost_csv)


# Stop the Spark session
spark.stop()
