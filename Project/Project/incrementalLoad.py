from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType
from pyspark.sql.functions import to_date, regexp_replace, col

#project path: /home/ec2-user/UKUSJul/vigu/Project/ -- in server

# Step 1: Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("CSVtoHive") \
    .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/user/ec2-user/UKUSJul/vigu/") \
    .enableHiveSupport() \
    .getOrCreate()


# Path to the CSV file
# patientcsv_file_path = "incrementLoad/patient1.csv"
medicalCost_file_path = "IncrementalLoad/medicalCost1.csv"
insurance_file_path = "IncrementalLoad/insurance1.csv"
hospitalTreatment_file_path = "IncrementalLoad/hospitalTreatment1.csv"


#no need to increment now
# ###########################################################################
# #Patient table and data creation START#
# ############################################################################
# #Define the schema for the dataFrme table 
# patientSchema = StructType([
#     StructField("patient_id", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("address", StringType(), True),
#     StructField("phone", StringType(), True),
#     StructField("email", StringType(), True)  # Initially read as StringType to convert later
# ])


# # Step 3: Read the CSV file into DataFrame with defined schema
# patientDf = spark.read.csv(patientcsv_file_path, schema=patientSchema, header=True)


# # Step 4: Create a Hive table with the specified schema
# patientTable_name = "patient_details"
# database_name = "default"  # You can specify your own database name

# # Use the appropriate Hive database
# spark.sql(f"USE {database_name}")

# # Step 5: Insert data into the Hive table
# patientDf.write.insertInto(patientTable_name, overwrite=True)

# # Step 6: Verify the data in the Hive table
# print("Data inserted into Hive table successfully. Showing the data:")
# spark.sql(f"SELECT * FROM {patientTable_name}").show()
# ###########################################################################
# #Patient table and data creation END#
# ############################################################################


###########################################################################
#medical cost table and data creation START#
############################################################################
medicalCostSchema = StructType([
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("bmi", FloatType(), True),
    StructField("children", IntegerType(), True),
    StructField("smoker", StringType(), True),
    StructField("region", StringType(), True),
    StructField("medical_cost", FloatType(), True),
    StructField("patient_id", StringType(), True)
])


# Step 3: Read the CSV file into DataFrame with defined schema
medicalCostDf = spark.read.csv(medicalCost_file_path, schema=medicalCostSchema, header=True)
medicalCostDf.show()


# Step 4: Create a Hive table with the specified schema
medical_costTable_name = "medical_cost"



# Step 5: Insert data into the Hive table
medicalCostDf.write.insertInto(medical_costTable_name)

# Step 6: Verify the data in the Hive table
print("Data inserted into Hive table successfully. Showing the data:")
spark.sql(f"SELECT * FROM {medical_costTable_name}").show()
###########################################################################
#Medicalcost table and data creation END#
############################################################################


# ###########################################################################
# #Insurance table and data creation START#
# ############################################################################
insuranceSchema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("insurance_company", StringType(), True),
    StructField("policy_number", StringType(), True),
    StructField("coverage_amount", FloatType(), True),
    StructField("insurance_cost", FloatType(), True)
   ])


# Step 3: Read the CSV file into DataFrame with defined schema
insuranceDf = spark.read.csv(insurance_file_path, schema=insuranceSchema, header=True)


# Step 4: Create a Hive table with the specified schema
insurance_Table_name = "insurance"



# Step 5: Insert data into the Hive table
insuranceDf.write.insertInto(insurance_Table_name)

# Step 6: Verify the data in the Hive table
print("Data inserted into Hive table successfully. Showing the data:")
spark.sql(f"SELECT * FROM {insurance_Table_name}").show()
###########################################################################
#Medicalcost table and data creation END#
############################################################################


###########################################################################
#hospitalTreatment table and data creation START#
############################################################################

# Step 2: Define the schema for the DataFrame based on the CSV structure
hospitalTreatmentschema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("treatment", StringType(), True),
    StructField("treatment_cost", FloatType(), True),
    StructField("hospital", StringType(), True),
    StructField("date_of_treatment", StringType(), True)  # Initially read as StringType to convert later
])


# Step 3: Read the CSV file into DataFrame with defined schema
hospitalTreatmentdf = spark.read.csv(hospitalTreatment_file_path, schema=hospitalTreatmentschema, header=True)

# Convert the 'date_of_treatment' column from string to date using to_date function
hospitalTreatmentdf = hospitalTreatmentdf.withColumn("date_of_treatment", to_date(regexp_replace("date_of_treatment", "\\s+", ""), "yyyy-MM-dd"))

# Display the DataFrame to verify conversion
hospitalTreatmentdf.show()

# Step 5: Create a Hive table with the specified schema
hospitalTreatmentTable_name = "hospital_treatment"
#database_name = "default"  # You can specify your own database name

# Use the appropriate Hive database
#spark.sql(f"USE {database_name}")


# Step 6: Insert data into the Hive table
# hospitalTreatmentdf.select("patient_id", "treatment", "treatment_cost", "hospital", "date_of_treatment") \
#   .write \
#   .mode("overwrite") \
#   .insertInto(hospitalTreatmentTable_name)

# Step 5: Insert data into the Hive table
hospitalTreatmentdf.write.insertInto(hospitalTreatmentTable_name)


# Step 7: Verify the data in the Hive table
print("Data inserted into Hive table successfully. Showing the data:")
spark.sql(f"SELECT * FROM {hospitalTreatmentTable_name}").show()

###########################################################################
#hospitalTreatment table and data creation End#
############################################################################

###########################################################################
# Join all DataFrames except patient_details to create the fact table
###########################################################################

# Step 1: Join the DataFrames on 'patient_id' to create a fact table
fact_df = medicalCostDf \
    .join(insuranceDf, on='patient_id', how='inner') \
    .join(hospitalTreatmentdf, on='patient_id', how='inner')

# Step 2: Select columns that you want in the fact table
fact_df = fact_df.select(
    col("patient_id"),
    col("age"),
    col("sex"),
    col("bmi"),
    col("children"),
    col("smoker"),
    col("region"),
    col("medical_cost"),
    col("insurance_company"),
    col("policy_number"),
    col("coverage_amount"),
    col("insurance_cost"),
    col("treatment"),
    col("treatment_cost"),
    col("hospital"),
    col("date_of_treatment")
)

# Step 3: Create the fact table in Hive
fact_table_name = "fact_medical_data"


# Step 4: Insert the data into the Hive table
fact_df.write.insertInto(fact_table_name)

# Step 5: Verify the data in the fact table
print("Data inserted into fact table successfully. Showing the data:")
spark.sql(f"SELECT * FROM {fact_table_name}").show()

###########################################################################
# Join all DataFrames except patient_details to create the fact table END
###########################################################################


# Step 8: Stop the Spark session
spark.stop()
