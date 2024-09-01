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

# Show loaded data
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

# Load existing data from Hive for merging
existing_patients_df = spark.table("julbatch.patients_table")
existing_hospital_treatment_df = spark.table("julbatch.hospital_treatment_table")
existing_insurance_df = spark.table("julbatch.insurance_table")
existing_medical_cost_df = spark.table("julbatch.medical_cost_table")

# Merge DataFrames for upsert logic
merged_patients_df = incremental_patients_df.alias("new").join(
    existing_patients_df.alias("existing"),
    "Patient_id",
    "left"
).select(
    coalesce("new.Patient_id", "existing.Patient_id").alias("Patient_id"),
    coalesce("new.Name", "existing.Name").alias("Name"),
    coalesce("new.Address", "existing.Address").alias("Address"),
    coalesce("new.Phone", "existing.Phone").alias("Phone"),
    coalesce("new.Email", "existing.Email").alias("Email")
)

# Similar merge logic for other DataFrames
merged_hospital_treatment_df = incremental_hospital_treatment_df.alias("new").join(
    existing_hospital_treatment_df.alias("existing"),
    "Patient_id",
    "left"
).select(
    coalesce("new.Patient_id", "existing.Patient_id").alias("Patient_id"),
    coalesce("new.treatment", "existing.treatment").alias("treatment"),
    coalesce("new.treatment_cost", "existing.treatment_cost").alias("treatment_cost"),
    coalesce("new.hospital", "existing.hospital").alias("hospital"),
    coalesce("new.date_of_treatment", "existing.date_of_treatment").alias("date_of_treatment")
)

# Similar merge logic for insurance and medical_cost DataFrames
merged_insurance_df = incremental_insurance_df.alias("new").join(
    existing_insurance_df.alias("existing"),
    "Patient_id",
    "left"
).select(
    coalesce("new.Patient_id", "existing.Patient_id").alias("Patient_id"),
    coalesce("new.insurance_company", "existing.insurance_company").alias("insurance_company"),
    coalesce("new.policy_number", "existing.policy_number").alias("policy_number"),
    coalesce("new.coverage_amount", "existing.coverage_amount").alias("coverage_amount"),
    coalesce("new.insurance_cost", "existing.insurance_cost").alias("insurance_cost")
)

merged_medical_cost_df = incremental_medical_cost_df.alias("new").join(
    existing_medical_cost_df.alias("existing"),
    "Patient_id",
    "left"
).select(
    coalesce("new.Patient_id", "existing.Patient_id").alias("Patient_id"),
    coalesce("new.age", "existing.age").alias("age"),
    coalesce("new.sex", "existing.sex").alias("sex"),
    coalesce("new.bmi", "existing.bmi").alias("bmi"),
    coalesce("new.children", "existing.children").alias("children"),
    coalesce("new.smoker", "existing.smoker").alias("smoker"),
    coalesce("new.region", "existing.region").alias("region"),
    coalesce("new.medical_cost", "existing.medical_cost").alias("medical_cost")
)

# Save merged data back to Hive tables
merged_patients_df.write.mode("overwrite").saveAsTable("julbatch.patients_table")
merged_hospital_treatment_df.write.mode("overwrite").saveAsTable("julbatch.hospital_treatment_table")
merged_insurance_df.write.mode("overwrite").saveAsTable("julbatch.insurance_table")
merged_medical_cost_df.write.mode("overwrite").saveAsTable("julbatch.medical_cost_table")

# Save merged data as CSV to provided output paths
merged_patients_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_patient_csv)
merged_hospital_treatment_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_hospital_treatment_csv)
merged_insurance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_insurance_csv)
merged_medical_cost_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_medical_cost_csv)

# Stop the Spark session
spark.stop()
