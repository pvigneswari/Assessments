from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

# Step 1: Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("CSVtoHive") \
    .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/user/ec2-user/UKUSJul/vigu/") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Define the schema for the DataFrame based on the CSV structure
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("treatment", StringType(), True),
    StructField("treatment_cost", FloatType(), True),
    StructField("hospital", StringType(), True),
    StructField("date_of_treatment", StringType(), True)  # Initially read as StringType to convert later
])

# Path to the CSV file
csv_file_path = "Project/medicalCost.csv"

# Step 3: Read the CSV file into DataFrame with defined schema
df = spark.read.csv(csv_file_path, schema=schema, header=True)

# Convert 'date_of_treatment' from StringType to DateType
df = df.withColumn("date_of_treatment", spark.sql.functions.to_date(df["date_of_treatment"], "dd-MM-yyyy"))

# Step 4: Create a Hive table with the specified schema
table_name = "hospital_treatment"
database_name = "default"  # You can specify your own database name

# Use the appropriate Hive database
spark.sql(f"USE {database_name}")

# Drop the table if it already exists to create a fresh schema
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Create the table with the desired schema in Hive
spark.sql(f"""
    CREATE TABLE {table_name} (
        patient_id STRING,
        treatment STRING,
        treatment_cost FLOAT,
        hospital STRING,
        date_of_treatment DATE
    )
    STORED AS PARQUET
""")

# Step 5: Insert data into the Hive table
df.write.insertInto(table_name, overwrite=True)

# Step 6: Verify the data in the Hive table
print("Data inserted into Hive table successfully. Showing the data:")
spark.sql(f"SELECT * FROM {table_name}").show()

# Step 7: Stop the Spark session
spark.stop()
