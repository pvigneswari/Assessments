import psycopg2
import pandas as pd
from psycopg2 import sql

# Database connection parameters
host = "ec2-18-132-73-146.eu-west-2.compute.amazonaws.com"     # Your PostgreSQL server address
port = "5432"          # Your PostgreSQL port
dbname = "testdb"   # Your database name
user = "consultants" # Your PostgreSQL username
password = "WelcomeItc@2022"   # Your PostgreSQL password



# Establish a connection to the database
try:
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = connection.cursor()
    print("Connected to the database successfully")
   
    # Specify the path to your Excel file
    file_path = 'Project\MedicalCost.xlsx'
    # Read the file_path
    file = pd.ExcelFile(file_path)
    sheetNames = file.sheet_names
    # Display the first few rows of the data
    # print(file.sheet_names)
    for sheetName in sheetNames:
        # Read all sheets
        table_name = sheetName.replace(' ','_').lower()

        data = pd.read_excel(file_path, sheet_name=sheetName)
        columnNames_array = [s.replace(" ", "_").lower() for s in data.columns.tolist()]
        # Generate SQL types for each column (you may need to customize this)
        sql_types = ["VARCHAR(255)" for _ in columnNames_array]  # Defaulting all columns to VARCHAR(255)
        columns = ",\n".join([f"{col.replace(' ', '_')} {sql_type}" for col, sql_type in zip(columnNames_array, sql_types)])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns}\n);"

        placeholders = ', '.join(['%s'] * len(columnNames_array))

       
       

        print("Generated SQL Query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        connection.commit()  # Commit the transaction
        print(f"Table '{table_name}' created successfully")

         # Step 3: Generate the SQL INSERT INTO statement dynamically
        insert_query = f"INSERT INTO {table_name} ({', '.join(columnNames_array)}) VALUES ({placeholders});"

        print("insert queri")
        print(insert_query)


        for index, row in data.iterrows():
            if index == 0:
                continue  # Skip the first row
            
            #####################################################################
            cursor.execute(insert_query, tuple(row))        
            connection.commit()  # Commit the transaction
            #####################################################################
            print(f"Data inserted into table '{table_name}' successfully")
       

    
except Exception as e:
    print("Error while connecting to PostgreSQL", e)
finally:
    # Close the database connection
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

