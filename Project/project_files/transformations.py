from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, avg, round, count, desc

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
         
        
        database_name = "default"  # Use the default Hive database or specify your own

        spark.sql("use " + database_name)
        # Execute the SQL command to create the table
        # spark.sql("show tables").show()

        fact_table_name = "fact_medical_data"
        fact_df = spark.sql("SELECT * FROM " + fact_table_name)


        # Step 3: Define age groups using DataFrame transformations
        fact_age_df = fact_df.withColumn(
            "age_group",
            when(col("age").between(0, 18), "0-18")
            .when(col("age").between(19, 35), "19-35")
            .when(col("age").between(36, 50), "36-50")
            .when(col("age").between(51, 65), "51-65")
            .otherwise("66+")
        )

        # Step 4: Calculate the average and total costs for each age group and service type
        age_group_analysis = fact_age_df.groupBy("age_group", "treatment") \
            .agg(
                round(avg("medical_cost"), 2).alias("avg_medical_cost"),
                round(sum("medical_cost"), 2).alias("total_medical_cost"),
                round(avg("treatment_cost"), 2).alias("avg_treatment_cost"),
                round(sum("treatment_cost"), 2).alias("total_treatment_cost")
            ) \
            .orderBy("age_group")

        # Step 5: Show the results
        age_group_analysis.show()
        print("\n Trend Analysis of Medical Costs by Age ---")
        print("- Analyze how medical costs vary across different age groups")
        print("  to identify which age groups incur the highest expenses")
        print("  and for which types of services.\n")



        ########################################################################

        # scenario 2: Define BMI categories and create a new column for BMI ranges
        ######################################################################

        fact_bmi_df = fact_df.withColumn(
            "bmi_category",
            when(col("bmi") < 18.5, "Underweight")
            .when((col("bmi") >= 18.5) & (col("bmi") < 25), "Normal weight")
            .when((col("bmi") >= 25) & (col("bmi") < 30), "Overweight")
            .otherwise("Obesity")
        )

        # Step 4: Calculate average medical costs for each BMI category
        bmi_cost_analysis = fact_bmi_df.groupBy("bmi_category") \
            .agg(
                round(avg("medical_cost"), 2).alias("avg_medical_cost"),
                count("medical_cost").alias("medical count")
            ) \
            .orderBy("bmi_category")

        # Step 5: Show the average medical costs for each BMI category
        print("Average medical costs by BMI category:")
        bmi_cost_analysis.show()
        print("\n--- Impact of BMI on Medical Costs ---")
        print(" Investigate the correlation between BMI and medical costs")
        print("  to understand how weight management programs could")
        print("  potentially reduce healthcare expenses.\n")

        # Step 6: Calculate the correlation between BMI and medical costs
        correlation_value = fact_df.stat.corr("bmi", "medical_cost")



        ########################################################################

        # scenario 3: Show the results for smokers vs non-smokers
        ######################################################################


        smoker_cost_analysis = fact_df.groupBy("smoker") \
        .agg(
            round(avg("medical_cost"), 2).alias("avg_medical_cost"),
            round(sum("medical_cost"), 2).alias("total_medical_cost"),
            count("medical_cost").alias("count")
        ) \
        .orderBy("smoker")

        # Step 4: Show the results for smokers vs non-smokers
        print("Medical costs comparison between smokers and non-smokers:")
        smoker_cost_analysis.show()
        print("\n--- Smoking and Medical Costs ---")
        print(" Compare the medical costs of smokers versus non-smokers")
        print("  to quantify the financial impact of smoking on healthcare systems.\n")

        #########################################################################

        # Step 3: Group data by age, BMI, smoking status, and region to calculate average medical costs
        premium_estimation_df = fact_df.groupBy("age", "smoker") \
            .agg(
                round(avg("coverage_amount"), 2).alias("avg_coverage_amount"),
                round(avg("treatment_cost"), 2).alias("avg_treatment_cost")                
            ) \
            .orderBy("age", "smoker")

        # Step 4: Show the estimated premiums based on risk factors
        print("Insurance Premium Estimation based on risk factors:")
        premium_estimation_df.show()   
        print("\n--- Insurance Premium Estimation ---")
        print(" Use the data to calculate potential insurance premiums based on risk factors")
        print("  such as age, smoking status, and region to ensure fair and accurate")
        print("  pricing of insurance policies.\n")

        #########################################################################

        # Step 3: Determine the threshold for high-cost patients (top 10% by medical cost)
        percentile_threshold = fact_df.approxQuantile("medical_cost", [0.9], 0.01)[0]  # 90th percentile

        # Step 4: Filter high-cost patients based on the determined threshold
        high_cost_patients_df = fact_df.filter(col("medical_cost") >= percentile_threshold)

        # Step 5: Analyze contributing factors for high-cost patients
        # Select relevant columns for analysis
        high_cost_analysis_df = high_cost_patients_df.select(
            "patient_id", "age", "bmi", "smoker", "region", "medical_cost", "treatment_cost", "treatment"
        ).orderBy(desc("medical_cost"))

        # Step 6: Show the results for high-cost patients
        print("High-Cost Patient Analysis - Patients with exceptionally high medical costs:")
        high_cost_analysis_df.show()
        print("\n--- High-Cost Patient Analysis ---")
        print(" Identify patients with exceptionally high medical costs and analyze")
        print("  the contributing factors to target interventions for cost reduction.\n")

        ############################################################################################

        # Step 3: Group data by the number of children to calculate average and total medical costs
        family_size_cost_analysis_df = fact_df.groupBy("children") \
            .agg(
                round(avg("medical_cost"), 2).alias("avg_medical_cost"),
                round(sum("medical_cost"), 2).alias("total_medical_cost"),
                count("medical_cost").alias("count")
            ) \
            .orderBy("children")

        family_size_cost_analysis_df.show()
        # Step 4: Show the results for medical costs by number of children
        print("\n--- Family Size and Medical Costs Analysis ---")
        print("This table provides an analysis of average and total medical costs based on the number of children in a family.")
        print("The data helps to understand the financial burden on larger families and the potential need for supportive healthcare policies.\n")

        


    except Exception as e:
        print("An error occurred while executing SQL commands:", e)

    finally:
        # Stop the Spark session
        spark.stop()    
