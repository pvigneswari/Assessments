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

# Load the new incremental datasets
patients_df = spark.read.option("header", "true").schema(patient_schema).csv(patient_csv)
hospital_treatment_df = spark.read.option("header", "true").schema(hospital_treatment_schema).csv(hospital_treatment_csv)
insurance_df = spark.read.option("header", "true").schema(insurance_schema).csv(insurance_csv)
medical_cost_df = spark.read.option("header", "true").schema(medical_cost_schema).csv(medical_cost_csv)
patients_df.show()
hospital_treatment_df.show()


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

# # Load existing data from PostgreSQL (if required for comparison or merging)
# existing_patients_df = spark.read.jdbc(url=postgres_url, table="patients_table", properties=postgres_properties)
# existing_hospital_treatment_df = spark.read.jdbc(url=postgres_url, table="hospital_treatment_table", properties=postgres_properties)
# existing_insurance_df = spark.read.jdbc(url=postgres_url, table="insurance_table", properties=postgres_properties)
# existing_medical_cost_df = spark.read.jdbc(url=postgres_url, table="medical_cost_table", properties=postgres_properties)



# Perform any filtering or transformations needed for incremental data
incremental_patients_df = patients_df  # Replace with filtering logic if needed
incremental_hospital_treatment_df = hospital_treatment_df  # Example filter
incremental_insurance_df = insurance_df  # Replace with filtering logic if needed
incremental_medical_cost_df = medical_cost_df  # Replace with filtering logic if needed


# Save incremental data to PostgreSQL
incremental_patients_df.write.jdbc(url=postgres_url, table="patients_table", mode="append", properties=postgres_properties)
incremental_hospital_treatment_df.write.jdbc(url=postgres_url, table="hospital_treatment_table", mode="append", properties=postgres_properties)
incremental_insurance_df.write.jdbc(url=postgres_url, table="insurance_table", mode="append", properties=postgres_properties)
incremental_medical_cost_df.write.jdbc(url=postgres_url, table="medical_cost_table", mode="append", properties=postgres_properties)
incremental_patients_df.show(10)
incremental_patients_df.printSchema()


# Save only top 10 records to Hive tables
incremental_patients_df.limit(10).write.mode("append").option("header","true").saveAsTable("julbatch.patients_table")
incremental_hospital_treatment_df.limit(10).write.mode("append").option("header","true").saveAsTable("julbatch.hospital_treatment_table")
incremental_insurance_df.limit(10).write.mode("append").option("header","true").saveAsTable("julbatch.insurance_table")
incremental_medical_cost_df.limit(10).write.mode("append").option("header","true").saveAsTable("julbatch.medical_cost_table")




# Read existing data from output folders if exists
try:
    existing_patients_df = spark.read.option("header", "true").schema(patient_schema).csv(output_patient_csv)
    incremental_patients_df = existing_patients_df.union(incremental_patients_df)
    incremental_patients_df.show()
except:
    print("No existing data found in", {output_patient_csv}, ". Proceeding with incremental data only.")

try:
    existing_hospital_treatment_df = spark.read.option("header", "true").schema(hospital_treatment_schema).csv(output_hospital_treatment_csv)
    incremental_hospital_treatment_df = existing_hospital_treatment_df.union(incremental_hospital_treatment_df)
    incremental_hospital_treatment_df.show()
except:
    print("No existing data found in ",{output_hospital_treatment_csv},". Proceeding with incremental data only.")

try:
    existing_insurance_df = spark.read.option("header", "true").schema(insurance_schema).csv(output_insurance_csv)
    incremental_insurance_df = existing_insurance_df.union(incremental_insurance_df)
except:
    print("No existing data found in ", {output_insurance_csv}, ". Proceeding with incremental data only.")

try:
    existing_medical_cost_df = spark.read.option("header", "true").schema(medical_cost_schema).csv(output_medical_cost_csv)
    incremental_medical_cost_df = existing_medical_cost_df.union(incremental_medical_cost_df)
except:
    print("No existing data found in", {output_medical_cost_csv},". Proceeding with incremental data only.")

# Coalesce to ensure a single output partition for each DataFrame
patients_cleaned_df_single_partition = incremental_patients_df.coalesce(1)
hospital_treatment_cleaned_df_single_partition = incremental_hospital_treatment_df.coalesce(1)
insurance_cleaned_df_single_partition = incremental_insurance_df.coalesce(1)
medical_cost_cleaned_df_single_partition = incremental_medical_cost_df.coalesce(1)

# Save coalesced dataframes to CSV files, overwriting the existing data
patients_cleaned_df_single_partition.write.mode("append").option("header", "true").csv(output_patient_csv)
hospital_treatment_cleaned_df_single_partition.write.mode("append").option("header", "true").csv(output_hospital_treatment_csv)
insurance_cleaned_df_single_partition.write.mode("append").option("header", "true").csv(output_insurance_csv)
medical_cost_cleaned_df_single_partition.write.mode("append").option("header", "true").csv(output_medical_cost_csv)

# Stop the Spark session
spark.stop()
