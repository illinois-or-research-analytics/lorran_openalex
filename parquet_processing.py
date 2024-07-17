import pandas as pd
import os
from tqdm import tqdm

# Directory containing the output Parquet files
output_directory = 'output/'

# Directory to store the final Parquet files
final_files_directory = 'final_files/'
os.makedirs(final_files_directory, exist_ok=True)

# Initialize lists to hold the edges and works
edges = []
works = []

# Walk through the output directory and read Parquet files
files_to_process = []
for subdir, _, files in os.walk(output_directory):
    for file in files:
        if file.endswith('_references.parquet') or file.endswith('_main.parquet'):
            files_to_process.append(os.path.join(subdir, file))

# Process files with a progress bar
for file_path in tqdm(files_to_process, desc="Processing files"):
    if file_path.endswith('_references.parquet'):
        print(f"Processing references file: {file_path}")

        # Read the references Parquet file
        try:
            references_df = pd.read_parquet(file_path, engine='pyarrow')
            # Extract openalex_id and reference_id columns and add to edges list
            edges.extend(references_df[['citing', 'cited']].values.tolist())
        except Exception as e:
            print(f"Error processing references file {file_path}: {e}")
    elif file_path.endswith('_main.parquet'):
        print(f"Processing main file: {file_path}")

        # Read the main Parquet file
        try:
            main_df = pd.read_parquet(file_path, engine='pyarrow')
            # Add the entire DataFrame to works list
            works.append(main_df)
        except Exception as e:
            print(f"Error processing main file {file_path}: {e}")

# Combine all works DataFrames into a single DataFrame
works_df = pd.concat(works, ignore_index=True)

# Create a DataFrame for the edge list
edge_list_df = pd.DataFrame(edges, columns=['citing', 'cited'])

# Paths to save the edge list and works Parquet files in the final_files directory
edge_list_file_path = os.path.join(final_files_directory, 'citation_network_edge_list.parquet')
works_file_path = os.path.join(final_files_directory, 'works.parquet')

# Save the edge list DataFrame to a Parquet file
edge_list_df.to_parquet(edge_list_file_path, index=False, engine='pyarrow')
print(f"Edge list saved to {edge_list_file_path}")

# Save the works DataFrame to a Parquet file
works_df.to_parquet(works_file_path, index=False, engine='pyarrow')
print(f"Works saved to {works_file_path}")
