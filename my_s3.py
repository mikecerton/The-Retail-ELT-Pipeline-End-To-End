import boto3
import pandas as pd
from io import StringIO

# S3 Configuration
bucket_name = "your-bucket-name"
file_key = "path/to/your-file.csv"

# Create an S3 client
s3 = boto3.client('s3')

# Fetch the file from S3
response = s3.get_object(Bucket=bucket_name, Key=file_key)

# Read the content of the file into a Pandas DataFrame
file_content = response['Body'].read().decode('utf-8')
df = pd.read_csv(StringIO(file_content))

# Show the DataFrame
print(df.head())
