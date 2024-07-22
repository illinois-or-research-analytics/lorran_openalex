import pandas as pd
import os
import time
from tqdm import tqdm

# Directory containing the Parquet files
input_directory = '../final_files/'

# Output file for the combined Parquet file
final_output_file = '../final_files2/node_table.parquet'

# Log file path
log_file_path = '../final_files2/processing_log.txt'

# Initialize a list to hold all openalex_id and DOI status values
all_openalex_ids = []

# Function to check if an openalex_id has a DOI
def check_has_doi(doi):
    return int(doi != 'DOI not available')

# List of Parquet files to process
parquet_files = [
    'works_chunk_0.parquet',
    'works_chunk_1.parquet',
    'works_chunk_2.parquet',
    'works_chunk_3.parquet',
    'works_chunk_4.parquet'
]

# Start timer
start_time = time.time()

# Read and process each Parquet file
for file in tqdm(parquet_files, desc="Processing Parquet files"):
    file_path = os.path.join(input_directory, file)
    print(f"Processing file: {file_path}")

    # Read the Parquet file
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        # Check DOI validity and prepare results
        df['hasDOI'] = df['DOI'].apply(check_has_doi)
        all_openalex_ids.extend(list(zip(df['openalex_id'], df['hasDOI'])))
        print(f"Processed {len(df)} records from {file_path}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

# Remove duplicates and sort the openalex_id values
all_openalex_ids = sorted(set(all_openalex_ids), key=lambda x: x[0])
print(f"Total unique records processed: {len(all_openalex_ids)}")

# Create a DataFrame with new_id values
new_id_df = pd.DataFrame(all_openalex_ids, columns=['openalex_id', 'hasDOI'])
new_id_df['new_id'] = range(len(new_id_df))
print("Created DataFrame with new_id values")

# Save the DataFrame to a Parquet file
os.makedirs(os.path.dirname(final_output_file), exist_ok=True)
new_id_df.to_parquet(final_output_file, index=False, engine='pyarrow')
print(f"Saved final Parquet file to {final_output_file}")

# End timer
end_time = time.time()
processing_time = end_time - start_time

# Log information
log_info = [
    f"Total time taken to process the data: {processing_time:.2f} seconds",
    f"Total unique records processed: {len(all_openalex_ids)}",
    f"Saved final Parquet file to {final_output_file}"
]

# Write log to file
with open(log_file_path, 'w') as log_file:
    for info in log_info:
        log_file.write(info + '\n')

print("Processing completed.")
print(f"Total time taken to process the data: {processing_time:.2f} seconds")
print(f"Total unique records processed: {len(all_openalex_ids)}")
print(f"Saved final Parquet file to {final_output_file}")
print(f"Log saved to {log_file_path}")
