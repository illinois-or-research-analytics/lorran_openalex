# creates "openalexID_newID_hasDOI.parquet" based on "works.parquet" (gather_works.py)
import pandas as pd
import time
import os

# Path to the final Parquet file created in the previous code
final_file_path = '../final_files/works.parquet'

# Path to save the new Parquet file
new_final_file_path = '../final_files/openalexID_integer_id_hasDOI.parquet'

# Path to the log file
log_file_path = '../final_files/processing_log_ids_table.txt'

# Initialize log file
with open(log_file_path, 'a') as log_file:
    log_file.write('Log file for processing errors and time measurements\n\n')

print("Reading the output Parquet file...")
start_time = time.time()

try:
    # Read the output Parquet file
    works_df = pd.read_parquet(final_file_path, engine='pyarrow')
    read_time = time.time() - start_time
    print(f"Read the output Parquet file in {read_time:.2f} seconds.")

    # Log the read time
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Read the output Parquet file in {read_time:.2f} seconds.\n")

    print("Creating 'hasDOI' column...")
    start_time = time.time()

    # Create the 'hasDOI' column based on whether the 'DOI' column has a valid DOI or not
    works_df['hasDOI'] = works_df['DOI'].apply(lambda x: 0 if x == 'DOI not available' else 1)
    create_column_time = time.time() - start_time
    print(f"Created 'hasDOI' column in {create_column_time:.2f} seconds.")

    # Log the column creation time
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Created 'hasDOI' column in {create_column_time:.2f} seconds.\n")

    print("Selecting required columns...")
    start_time = time.time()

    # Select the required columns
    selected_df = works_df[['openalex_id', 'new_id', 'hasDOI']].rename(columns={'new_id': 'integer_id'})
    select_columns_time = time.time() - start_time
    print(f"Selected required columns in {select_columns_time:.2f} seconds.")

    # Log the column selection time
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Selected required columns in {select_columns_time:.2f} seconds.\n")

    print("Saving the new DataFrame to a Parquet file...")
    start_time = time.time()

    # Save the new DataFrame to a Parquet file
    selected_df.to_parquet(new_final_file_path, index=False, engine='pyarrow')
    save_time = time.time() - start_time
    print(f"Saved the new DataFrame to {new_final_file_path} in {save_time:.2f} seconds.")

    # Log the save time
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Saved the new DataFrame to {new_final_file_path} in {save_time:.2f} seconds.\n")

except Exception as e:
    # Log any errors
    error_time = time.time() - start_time
    print(f"Error occurred: {e}")
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Error occurred: {e}\n")
        log_file.write(f"Time taken before error: {error_time:.2f} seconds\n")
