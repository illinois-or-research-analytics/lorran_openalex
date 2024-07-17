######################################
# Simple code to check the file      #
######################################

import pandas as pd

file_path = 'output/updated_date=2024-06-30/part_000_references.parquet' # final_files/citation_network_edge_list.parquet
df = pd.read_parquet(file_path, engine='pyarrow') 

print("Column names:", df.columns.tolist())
print(df.head())

file_path = 'output/updated_date=2024-06-30/part_000_main.parquet' # final_files/works.parquet
df = pd.read_parquet(file_path, engine='pyarrow') 

print("Column names:", df.columns.tolist())
print(df.head())