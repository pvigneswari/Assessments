import psycopg2


# Database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'August@12345678',
    'host': 'localhost',
    'port': '5432'  # Default PostgreSQL port
}

# Function to perform incremental load from customer to customer_backup
def incremental_load_customer_to_backup():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Perform the MERGE operation to synchronize customer_backup with customer
        merge_query = """
        MERGE INTO customer_backup AS backup
        USING customer AS cust
        ON backup.cid = cust.cid
        WHEN MATCHED THEN
          UPDATE SET name = cust.name, email = cust.email, lastChange = cust.lastChange
        WHEN NOT MATCHED THEN
           INSERT (cid, name, email, lastChange) VALUES (cust.cid, cust.name, cust.email, cust.lastChange);
        """
        
        cursor.execute(merge_query)

        # Commit the transaction
        conn.commit()

        print("Incremental load from customer to customer_backup completed successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Execute the function
incremental_load_customer_to_backup()