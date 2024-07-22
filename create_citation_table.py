import pandas as pd
import os
import time
from tqdm import tqdm

# Directory containing the citation Parquet files
citation_directory = '../final_files/'

# File containing the ID and has_DOI information
id_info_file = '../final_files_/openalexID_newID_hasDOI.parquet'

# Output directory for the citation table Parquet file
output_directory = '../final_files5/'
os.makedirs(output_directory, exist_ok=True)
citation_output_file = os.path.join(output_directory, 'citation_table_chunks.parquet')

# Log file path
log_file_path = os.path.join(output_directory, 'citation_processing_log_chunks.txt')

# Initialize lists to hold the citation data
citation_data = []
chunk_size = 100000000  # Number of rows per chunk
chunk_counter = 0

# Start timer
start_time = time.time()

# Read the ID and has_DOI information
print("Reading ID and hasDOI information...")
id_info_df = pd.read_parquet(id_info_file, engine='pyarrow')
print("Finished reading ID and hasDOI information.")

# Rename columns in id_info_df for merging
id_info_df = id_info_df.rename(columns={'openalex_id': 'id', 'new_id': 'new_id', 'hasDOI': 'hasDOI'})

# Process each citation Parquet file
print("Processing citation files...")
for i in tqdm(range(21), desc="Processing citation files"):
    file = f'citation_network_edge_list_chunk_{i}.parquet'
    file_path = os.path.join(citation_directory, file)
    print(f"Processing file: {file_path}")

    # Read the citation Parquet file
    try:
        citation_df = pd.read_parquet(file_path, engine='pyarrow')

        # Merge to get new IDs and hasDOI for citing
        print(f"Merging citing IDs for file: {file_path}")
        citation_df = citation_df.merge(id_info_df, left_on='citing', right_on='id', how='left', suffixes=('', '_citing'))
        citation_df = citation_df.rename(columns={'new_id': 'citing_new_id', 'hasDOI': 'citing_hasDOI'})
        print(f"Finished merging citing IDs for file: {file_path}")

        # Merge to get new IDs and hasDOI for cited
        print(f"Merging cited IDs for file: {file_path}")
        citation_df = citation_df.merge(id_info_df, left_on='cited', right_on='id', how='left', suffixes=('', '_cited'))
        citation_df = citation_df.rename(columns={'new_id': 'cited_new_id', 'hasDOI': 'cited_hasDOI'})
        print(f"Finished merging cited IDs for file: {file_path}")

        # Select relevant columns
        citation_df = citation_df[['citing_new_id', 'citing_hasDOI', 'cited_new_id', 'cited_hasDOI']]

        # Append to citation_data
        citation_data.extend(citation_df.values.tolist())

        if len(citation_data) >= chunk_size:
            citation_chunk_df = pd.DataFrame(citation_data, columns=['citing', 'citing_hasDOI', 'cited', 'cited_hasDOI'])
            chunk_output_file = os.path.join(output_directory, f'citation_table_chunk_{chunk_counter}.parquet')
            citation_chunk_df.to_parquet(chunk_output_file, index=False, engine='pyarrow')
            print(f"Saved chunk {chunk_counter} with {len(citation_chunk_df)} rows to {chunk_output_file}")
            citation_data = []
            chunk_counter += 1

        print(f"Processed {len(citation_df)} records from {file_path}")
    except Exception as e:
        error_message = f"Error processing file {file_path}: {e}"
        print(error_message)
        with open(log_file_path, 'a') as log_file:
            log_file.write(error_message + '\n')

# Save any remaining data
if citation_data:
    citation_chunk_df = pd.DataFrame(citation_data, columns=['citing', 'citing_hasDOI', 'cited', 'cited_hasDOI'])
    chunk_output_file = os.path.join(output_directory, f'citation_table_chunk_{chunk_counter}.parquet')
    citation_chunk_df.to_parquet(chunk_output_file, index=False, engine='pyarrow')
    print(f"Saved final chunk {chunk_counter} with {len(citation_chunk_df)} rows to {chunk_output_file}")

# End timer
end_time = time.time()
processing_time = end_time - start_time

# Log information
log_info = [
    f"Total time taken to process the data: {processing_time:.2f} seconds",
    f"Total number of chunks created: {chunk_counter + 1}",
    f"Saved final Parquet file chunks to {output_directory}"
]

# Write log to file
with open(log_file_path, 'a') as log_file:
    for info in log_info:
        log_file.write(info + '\n')

print("Processing completed.")
print(f"Total time taken to process the data: {processing_time:.2f} seconds")
print(f"Total number of chunks created: {chunk_counter + 1}")
print(f"Saved final Parquet file chunks to {output_directory}")
print(f"Log saved to {log_file_path}")
