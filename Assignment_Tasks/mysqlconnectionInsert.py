import mysql.connector
from mysql.connector import Error

# Database connection details
host = 'localhost'
user = 'root'
password = 'August@12345678'
database = 'challenge1'

def insert_sales_row(customer_id, order_date, product_id):
    try:
        # Establish the connection
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        if connection.is_connected():
            print("Successfully connected to the database")
            
            cursor = connection.cursor()
            
            # Query to insert data into sales table
            query = "INSERT INTO sales (customer_id, order_date, product_id) VALUES (%s, %s, %s)"
            data = (customer_id, order_date, product_id)
            
            cursor.execute(query, data)
            connection.commit()  # Commit the transaction
            
            print("Data inserted successfully into sales table")
    
    except Error as err:
        print(f"Error: {err}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    # Collecting data from user
    customer_id = input("Enter customer_id (e.g., A or B or C): ")
    order_date = input("Enter order_date (YYYY-MM-DD): ")
    product_id = int(input("Enter product_id (e.g., 1): "))
    
    # Insert the collected data into sales table
    insert_sales_row(customer_id, order_date, product_id)