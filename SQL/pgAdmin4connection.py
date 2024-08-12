import psycopg2


# Database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'August@12345678',
    'host': 'localhost',
    'port': '5432'
}

# Function to perform full load from customer to customer_backup
def full_load_customer_to_backup():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Clear the customer_backup table
        truncate_query = "TRUNCATE TABLE customer_backup"
        cursor.execute(truncate_query)

        # Copy data from customer to customer_backup
        copy_query = """
        INSERT INTO customer_backup (cid, name, email, lastChange)
        SELECT cid, name, email, lastChange FROM customer
        """
        cursor.execute(copy_query)

        # Commit the transaction
        conn.commit()

        print("Full load from customer to customer_backup completed successfully.")

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
full_load_customer_to_backup()