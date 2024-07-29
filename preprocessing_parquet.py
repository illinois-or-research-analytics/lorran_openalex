import pandas as pd
import os
from tqdm import tqdm

# Directory containing the output Parquet files
output_directory = '../output/'

# Directory to store the final Parquet files
final_files_directory = '../preprocessed_files/'
os.makedirs(final_files_directory, exist_ok=True)

# Temporary storage for edges and works chunks
edges_chunks = []
works_chunks = []

# Function to save chunks of data
def save_chunk(data, filename, chunk_counter, final_files_directory):
    chunk_filename = os.path.join(final_files_directory, f"{filename}_chunk_{chunk_counter}.parquet")
    data.to_parquet(chunk_filename, index=False, engine='pyarrow')
    print(f"Saved {filename} chunk {chunk_counter} to {chunk_filename}")

# Initialize counters
edges_chunk_counter = 0
works_chunk_counter = 0
chunk_size = 100000000  # Adjustble

# Walk through the output directory and read Parquet files
files_to_process = []
for subdir, _, files in os.walk(output_directory):
    for file in files:
        if file.endswith('_references.parquet'): # or file.endswith('_main.parquet'):
            files_to_process.append(os.path.join(subdir, file))

# Process files with a progress bar
for file_path in tqdm(files_to_process, desc="Processing files"):
    if file_path.endswith('_references.parquet'):
        print(f"Processing references file: {file_path}")

        # Read the references Parquet file
        try:
            references_df = pd.read_parquet(file_path, engine='pyarrow')
            # Extract openalex_id and reference_id columns and add to edges list
            edges_chunks.extend(references_df[['citing', 'cited']].values.tolist())

            # Save chunk if it exceeds the chunk size
            if len(edges_chunks) >= chunk_size:
                edges_df = pd.DataFrame(edges_chunks, columns=['citing', 'cited'])
                save_chunk(edges_df, 'citation_network_edge_list', edges_chunk_counter, final_files_directory)
                edges_chunk_counter += 1
                edges_chunks = []

        except Exception as e:
            print(f"Error processing references file {file_path}: {e}")
    # elif file_path.endswith('_main.parquet'):
    #     print(f"Processing main file: {file_path}")

    #     # Read the main Parquet file
    #     try:
    #         main_df = pd.read_parquet(file_path, engine='pyarrow')
    #         works_chunks.append(main_df)

    #         # Save chunk if it exceeds the chunk size
    #         if len(works_chunks) * main_df.shape[0] >= chunk_size:
    #             works_df = pd.concat(works_chunks, ignore_index=True)
    #             save_chunk(works_df, 'works', works_chunk_counter, final_files_directory)
    #             works_chunk_counter += 1
    #             works_chunks = []

    #     except Exception as e:
    #         print(f"Error processing main file {file_path}: {e}")

# Save remaining edges chunks if any
if edges_chunks:
    edges_df = pd.DataFrame(edges_chunks, columns=['citing', 'cited'])
    save_chunk(edges_df, 'citation_network_edge_list', edges_chunk_counter, final_files_directory)

# Save remaining works chunks if any
# if works_chunks:
#     works_df = pd.concat(works_chunks, ignore_index=True)
#     save_chunk(works_df, 'works', works_chunk_counter, final_files_directory)