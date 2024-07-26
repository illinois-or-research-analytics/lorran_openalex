# creates "works.parquet"

import pandas as pd
import os
import time
from tqdm import tqdm

# Directory containing the output Parquet files
output_directory = '../output/'

# Directory to store the final Parquet files
final_files_directory = '../final_files/'
os.makedirs(final_files_directory, exist_ok=True)

# Path to the log file
log_file_path = os.path.join(final_files_directory, 'processing_log.txt')

# Temporary storage for works data
works_data = []

# Initialize log file
with open(log_file_path, 'w') as log_file:
    log_file.write('Log file for processing errors and time measurements\n\n')

# Walk through the output directory and read Parquet files
files_to_process = []
for subdir, _, files in os.walk(output_directory):
    for file in files:
        if file.endswith('_main.parquet'):
            files_to_process.append(os.path.join(subdir, file))

# Process files with a progress bar
for file_path in tqdm(files_to_process, desc="Processing files"):
    print(f"Processing main file: {file_path}")

    start_time = time.time()

    # Read the main Parquet file
    try:
        main_df = pd.read_parquet(file_path, engine='pyarrow')
        works_data.append(main_df)

        # Log processing time
        end_time = time.time()
        processing_time = end_time - start_time
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Processed file {file_path} in {processing_time:.2f} seconds\n")

    except Exception as e:
        # Log error
        end_time = time.time()
        processing_time = end_time - start_time
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Error processing file {file_path}: {e}\n")
            log_file.write(f"Time taken before error: {processing_time:.2f} seconds\n")

# Concatenate all works data
if works_data:
    start_time = time.time()
    try:
        works_df = pd.concat(works_data, ignore_index=True)
        works_df = works_df.sort_values(by='openalex_id').reset_index(drop=True)
        works_df['new_id'] = range(len(works_df))
        final_file_path = os.path.join(final_files_directory, 'works.parquet')
        works_df.to_parquet(final_file_path, index=False, engine='pyarrow')
        print(f"Saved all works data to {final_file_path}")

        # Log processing time
        end_time = time.time()
        processing_time = end_time - start_time
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Processed and saved all works data in {processing_time:.2f} seconds\n")

    except Exception as e:
        # Log error
        end_time = time.time()
        processing_time = end_time - start_time
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Error processing and saving all works data: {e}\n")
            log_file.write(f"Time taken before error: {processing_time:.2f} seconds\n")
