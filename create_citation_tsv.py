import pandas as pd
import os
import time

# File path to the Parquet file
file_path = '../final_files/citation_table.parquet'  
print("Starting...")
df = pd.read_parquet(file_path, engine='pyarrow')
print("df loaded")
# Extract the required columns
df_selected = df[['citing', 'cited']]
print("required columns extracted")
# Define the output file path
output_file_path = '../final_files/citation_table.tsv' 
# Save to TSV without header
print("starting to save the new file...")
df_selected.to_csv(output_file_path, sep='\t', index=False, header=False)
print(f"File saved to {output_file_path}")