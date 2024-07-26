######################################
# Simple code to check the file      #
######################################
import pandas as pd
import os
import time
#from tqdm import tqdm

# File path to the Parquet file
file_path = '../final_files/combined_citation_table.parquet'  # Adjust the path as needed

# Start timer
start_time = time.time()
# Read the Parquet file
print("Starting...")
df = pd.read_parquet(file_path, engine='pyarrow')
# End timer
end_time = time.time()
read_time = end_time - start_time

# Print the time taken to read the file
print(f"Time taken to read the file: {read_time:.2f} seconds")

# Print column names and the first few rows
print("Column names:", df.columns.tolist())
print(f"The DataFrame has {df.shape[0]} rows.")
print(df.dtypes)
print(df.head())

#---------------------------------------------------
