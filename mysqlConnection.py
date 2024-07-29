import mysql.connector


# Replace with your database credentials
config = {
    'user': 'root',
    'password': 'August@12345678',
    'host': 'localhost',
    'database': 'challenge1'
}

connection = mysql.connector.connect(**config)

# if connection.is_connected():
#     print("Connection successful")
# else:
#     print("Connection failed")


cursor = connection.cursor()

#  query
cursor.execute("SELECT * FROM sales")

# Fetch results
results = cursor.fetchall()

for row in results:
    print(row)


# Close the cursor and connection
cursor.close()
connection.close()
