import gzip
import json
import pandas as pd
import time
import re
import os

# Directory containing the part files
directory_path = 'data/works/'

# Function to extract relevant information from each JSON line
def extract_info(json_line):
    data = json.loads(json_line)
    openalex_id = data.get('id', '').replace('https://openalex.org/', '')
    
    # Simplified regular expression to validate the beginning of the DOI
    doi_pattern = re.compile(r'^https://doi\.org/')
    doi = data.get('doi', None)
    if doi is None or not doi_pattern.match(doi):
        doi = 'DOI not available'
        
    publication_year = data.get('publication_year', 'Year not available')
    references = [ref.replace('https://openalex.org/', '') for ref in data.get('referenced_works', [])]
    author_count = len(data.get('authorships', []))
    return openalex_id, doi, publication_year, references, author_count

# Initialize lists to hold the extracted data
ids, dois, publication_years, author_counts = [], [], [], []
references_data = []
no_doi_count = 0  # Counter for lines without DOI

# Start timing
start_time = time.time()

# Process each part file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith('.gz'):
        file_path = os.path.join(directory_path, filename)
        
        # Read the file and extract information
        with gzip.open(file_path, 'rt') as f:
            for line in f:
                openalex_id, doi, publication_year, references, author_count = extract_info(line)
                ids.append(openalex_id)
                dois.append(doi)
                publication_years.append(publication_year)
                author_counts.append(author_count)
                
                if doi == 'DOI not available':
                    no_doi_count += 1
                
                for ref in references:
                    references_data.append({'openalex_id': openalex_id, 'reference_id': ref})

# Calculate the time taken
end_time = time.time()
processing_time = end_time - start_time

# Create DataFrames from the extracted data
main_df = pd.DataFrame({
    'openalex_id': ids,
    'DOI': dois,
    'publication_year': publication_years,
    'author_count': author_counts
})

references_df = pd.DataFrame(references_data)

# Save the DataFrames to CSV files
main_output_file_path = 'openalex_main_dataset.csv'
references_output_file_path = 'openalex_references_dataset.csv'
main_df.to_csv(main_output_file_path, index=False)
references_df.to_csv(references_output_file_path, index=False)

# Print the first few rows of the main DataFrame
print(main_df.head())

# Print the number of lines without a valid DOI
print(f"Number of lines without a valid DOI: {no_doi_count}")

# Print the total number of works processed
print(f"Total number of works processed: {len(ids)}")

# Print the processing time
print(f"Time taken to process the data: {processing_time:.2f} seconds")