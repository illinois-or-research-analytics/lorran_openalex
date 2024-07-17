# data source: https://openalex.s3.amazonaws.com/browse.html#data/works/updated_date=2024-06-30/
# data file: part_000.gz

# create a data file where:
# ID: openalex_id
# Collumns: DOI (add something in case info is missing)| publications_year | references (a list) | author_count

import gzip
import json
import pandas as pd
import time
import re

# File path
file_path = 'data/works/part_000.gz'  # It has 220.270 lines/works

# Function to extract relevant information from each JSON line
def extract_info(json_line):
    data = json.loads(json_line)
    openalex_id = data.get('id', '').replace('https://openalex.org/', '')
    doi_pattern = re.compile(r'^https://doi\.org/', re.IGNORECASE)
    doi = data.get('doi', None)
    if doi is None or not doi_pattern.match(doi):
        doi = 'DOI not available'
    publication_year = data.get('publication_year', 'Year not available')
    references = [ref.replace('https://openalex.org/', '') for ref in data.get('referenced_works', [])]
    author_count = len(data.get('authorships', []))
    return openalex_id, doi, publication_year, references, author_count

# Initialize lists to hold the extracted data
ids, dois, publication_years, references_list, author_counts = [], [], [], [], []
no_doi_count = 0  # Counter for lines without DOI

# Start timing
start_time = time.time()

# Read the file and extract information for the first 10,000 lines
with gzip.open(file_path, 'rt') as f:
    for i, line in enumerate(f):
        # if i >= 10000:
        #     break
        openalex_id, doi, publication_year, references, author_count = extract_info(line)
        ids.append(openalex_id)
        dois.append(doi)
        publication_years.append(publication_year)
        references_list.append(references)
        author_counts.append(author_count)
        
        if doi == 'DOI not available':
            no_doi_count += 1

# Calculate the time taken
end_time = time.time()
processing_time = end_time - start_time

# Create a DataFrame from the extracted data
df = pd.DataFrame({
    'openalex_id': ids,
    'DOI': dois,
    'publication_year': publication_years,
    'references': references_list,
    'author_count': author_counts
})

# Save the DataFrame to a CSV file
output_file_path = 'openalex_dataset_10k.csv'    # 91 seconds for 220k, Number of lines without DOI: 5204 (2.36%)
                                                  # 4 seconds for 10k, Number of lines without DOI: 348 (3.48%) 
df.to_csv(output_file_path, index=False)

# Print the first few rows of the DataFrame
print(df.head())

# Print the number of lines without a DOI
print(f"Number of lines without DOI: {no_doi_count}")

# Print the processing time
print(f"Time taken to process the data: {processing_time:.2f} seconds")