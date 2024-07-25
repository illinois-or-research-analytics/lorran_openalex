import pandas as pd
import os
import time
from tqdm import tqdm

# Directory containing the citation chunk Parquet files
chunk_directory = '../final_files6/'

# Output file for the combined citation table Parquet file
combined_output_file = os.path.join(chunk_directory, 'combined_citation_table.parquet')

# Log file path
log_file_path = os.path.join(chunk_directory, 'combine_chunks_log.txt')

# Start timer
start_time = time.time()

# Initialize list to hold DataFrames
dataframes = []

# Process each chunk Parquet file
print("Combining chunk files...")
for i in tqdm(range(20), desc="Combining chunks"):
    file = f'citation_table_chunk_{i}.parquet'
    file_path = os.path.join(chunk_directory, file)
    print(f"Reading file: {file_path}")
    
    try:
        chunk_df = pd.read_parquet(file_path, engine='pyarrow')
        dataframes.append(chunk_df)
        print(f"Appended {len(chunk_df)} records from {file_path}")
    except Exception as e:
        error_message = f"Error reading file {file_path}: {e}"
        print(error_message)
        with open(log_file_path, 'a') as log_file:
            log_file.write(error_message + '\n')

# Concatenate all DataFrames
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined DataFrame to a Parquet file
print(f"Saving combined DataFrame to {combined_output_file}...")
combined_df.to_parquet(combined_output_file, index=False, engine='pyarrow')
print(f"Saved combined citation table Parquet file to {combined_output_file}")

# End timer
end_time = time.time()
processing_time = end_time - start_time

# Log information
log_info = [
    f"Total time taken to combine the data: {processing_time:.2f} seconds",
    f"Total number of records in combined citation table: {len(combined_df)}",
    f"Saved combined Parquet file to {combined_output_file}"
]

# Write log to file
with open(log_file_path, 'a') as log_file:
    for info in log_info:
        log_file.write(info + '\n')

print("Combining completed.")
print(f"Total time taken to combine the data: {processing_time:.2f} seconds")
print(f"Total number of records in combined citation table: {len(combined_df)}")
print(f"Saved combined Parquet file to {combined_output_file}")
print(f"Log saved to {log_file_path}")
