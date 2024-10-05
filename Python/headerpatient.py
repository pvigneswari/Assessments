import pandas as pd

# Load the uploaded CSV file
file_path = 'C:/workplace/Assessments/Project/AWS/Datasets/patient.csv'

# Read the CSV file to examine the content and check for header issues
df = pd.read_csv(file_path)
# print(df)

# Display the first few rows of the CSV to see if headers are correctly aligned
print(df.head())


