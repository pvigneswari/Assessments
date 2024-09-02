from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Step 1: Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("CSVtoHiveUpdateDelete") \
    .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Define the schema for the DataFrame based on the CSV structure
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True)
])

# Path to the CSV file
csv_file_path = "patient.csv"  # Ensure this is the correct path in your environment

# Step 3: Read the CSV file into DataFrame with defined schema
df = spark.read.csv(csv_file_path, schema=schema, header=True)

# Step 4: Create a Hive table with the specified schema
table_name = "patient_data"
database_name = "default"  # Use default database or specify your own

# Use the appropriate Hive database
spark.sql(f"USE {database_name}")

# Drop the table if it already exists to create a fresh schema
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Create the table with bucketing and ORC format for ACID compliance
spark.sql(f"""
    CREATE TABLE {table_name} (
        patient_id STRING,
        name STRING,
        address STRING,
        phone STRING,
        email STRING
    )
    CLUSTERED BY (patient_id) INTO 4 BUCKETS
    STORED AS ORC
    TBLPROPERTIES ('transactional'='true')
""")

# Step 5: Insert data into the Hive table using bucketing
# Write DataFrame to Hive table in ORC format with bucketing
df.write \
  .format("orc") \
  .mode("overwrite") \
  .bucketBy(4, "patient_id") \
  .saveAsTable(table_name)

# Step 6: Perform update operation
# Example: Update email for a specific patient_id
spark.sql(f"""
    UPDATE {table_name} 
    SET email = 'updated_email@example.com' 
    WHERE patient_id = 'P0002';
""")

# Step 7: Perform delete operation
# Example: Delete records where patient name is 'John Doe'
spark.sql(f"""
    DELETE FROM {table_name} 
    WHERE name = 'John Doe';
""")

# Step 8: Verify the data in the Hive table after update and delete
print("Data in Hive table after update and delete:")
spark.sql(f"SELECT * FROM {table_name}").show()

# Step 9: Stop the Spark session
spark.stop()
