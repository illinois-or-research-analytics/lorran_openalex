######################################
# Simple code to check the file      #
######################################
import pandas as pd
import os
import time
from tqdm import tqdm

# # File path to the Parquet file
# file_path = '../final_files5/citation_table_chunk_1.parquet'  # Adjust the path as needed

# # Start timer
# start_time = time.time()
# # Read the Parquet file
# df = pd.read_parquet(file_path, engine='pyarrow')
# # End timer
# end_time = time.time()
# read_time = end_time - start_time

# # Print the time taken to read the file
# print(f"Time taken to read the file: {read_time:.2f} seconds")

# # Print column names and the first few rows
# print("Column names:", df.columns.tolist())
# print(df.head())


# Directory containing the Parquet chunk files
chunk_directory = '../final_files5/'

# Initialize counters
total_lines = 0
total_nan_lines = 0

# Start timer
start_time = time.time()

# Read and process each chunk file
for i in tqdm(range(21), desc="Processing chunk files"):
    file_path = os.path.join(chunk_directory, f'citation_table_chunk_{i}.parquet')
    if os.path.exists(file_path):
        print(f"Processing file: {file_path}")
        
        # Start reading timer
        read_start_time = time.time()
        
        # Read the Parquet file
        df = pd.read_parquet(file_path, engine='pyarrow')
        
        # End reading timer
        read_end_time = time.time()
        read_time = read_end_time - read_start_time
        
        # Count total lines and NaN lines
        total_lines += len(df)
        total_nan_lines += df.isnull().any(axis=1).sum()
        
        # Print the time taken to read the file
        print(f"Time taken to read {file_path}: {read_time:.2f} seconds")

# Calculate the percentage of lines with NaN values
percent_nan = (total_nan_lines / total_lines) * 100 if total_lines else 0

# End timer
end_time = time.time()
processing_time = end_time - start_time

# Print the results
print(f"Total number of lines: {total_lines}")
print(f"Total number of lines with NaN values: {total_nan_lines}")
print(f"Percentage of lines with NaN values: {percent_nan:.2f}%")
print(f"Total time taken to process the data: {processing_time:.2f} seconds")

# Total number of lines: 2155553273
# Total number of lines with NaN values: 6765125
# Percentage of lines with NaN values: 0.31%
# Total time taken to process the data: 98.12 seconds