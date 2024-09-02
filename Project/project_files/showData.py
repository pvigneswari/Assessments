from pyspark.sql import SparkSession

# Function to create a Spark session with Hive support
def create_spark_session():
    # Create a Spark session with Hive support enabled
    spark = SparkSession.builder \
        .appName("HiveIntegrationApp") \
        .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/user/ec2-user/UKUSJul/vigu/") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session()
    
    # Specify the Hive database you want to use
    database_name = "default"  # Replace with your actual database name

    try:
         # Step 2: Create a Hive table based on the DataFrame schema
        database_name = "default"  # Use the default Hive database or specify your own

        # Switch to the specified Hive database
        databases = spark.sql("SHOW databases")
        databases.show()
        #spark.sql(f"use database {database_name}")
        # Execute the SQL command to create the table
        spark.sql("use default")
        showtafbles = spark.sql("show tables")
        showtafbles.show()

        spark.sql("""
            SELECT 
                a.medical_cost_count, 
                b.insurance_count,
                c.hospital_treatment_count,
                d.patient_details_count   
            FROM
                (SELECT COUNT(*) AS medical_cost_count FROM medical_cost) a
            CROSS JOIN 
                (SELECT COUNT(*) AS insurance_count FROM insurance) b
            CROSS JOIN 
                (SELECT COUNT(*) AS hospital_treatment_count FROM hospital_treatment) c
            CROSS JOIN       
                  (SELECT COUNT(*) AS patient_details_count FROM patient_details) d                        
        """).show()

        # spark.sql("""
        #     SELECT 
        #         a.medical_cost_count, 
        #         b.insurance_count,
        #         c.hospital_treatment_count,
        #           d.fact_medical_data_count   
        #     FROM 
        #         (SELECT COUNT(*) AS medical_cost_count FROM medical_cost) a
        #     CROSS JOIN 
        #         (SELECT COUNT(*) AS insurance_count FROM insurance) b
        #     CROSS JOIN 
        #         (SELECT COUNT(*) AS hospital_treatment_count FROM hospital_treatment) c
        #     CROSS JOIN 
        #         (SELECT COUNT(*) AS fact_medical_data_count FROM fact_medical_data) d                        
        # """).show()
        

        # spark.sql("ALTER TABLE hospitalTreatment CHANGE COLUMN treatment_cost treatment_cost FLOAT;").show()
        # spark.sql("select sum(treatment_cost) as sumOfTreatmentCost from default.hospitalTreatment limit 10").show()
        # spark.sql("""
        #     SELECT SUM(treatment_cost) AS sumOfTreatmentCost 
        #     FROM (
        #         SELECT treatment_cost 
        #         FROM default.hospital_treatment 
        #         where date_of_treatment between  '2015-01-01' and '2016-12-31' limit 2
        #     ) AS subquery
        # """).show()
        
        # spark.sql("""
        #           SELECT * 
        #         FROM default.hospital_treatment where date_of_treatment between  '2015-01-01' and '2016-12-31'
        # """).show()

        # spark.sql("describe  hospitalTreatment").show()

    except Exception as e:
        print("An error occurred while executing SQL commands:", e)

    finally:
        # Stop the Spark session
        spark.stop()    
