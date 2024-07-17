import gzip
import json
import pandas as pd
import time
import re
import os

# Directory containing the part files
directory_path = 'openalex-snapshot/data/works/'

# Output directory for Parquet files
output_directory = 'output/'
os.makedirs(output_directory, exist_ok=True)

# Log file path
log_file_path = os.path.join(output_directory, 'processing_log.txt')

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

# Initialize counters for lines with and without DOI
total_no_doi_count = 0
total_lines_processed = 0
errors = []

# Start timing
start_time = time.time()

# Sort the subdirectories
subdirectories = sorted(os.listdir(directory_path))

# Process each subdirectory and part file in the directory
for subdirectory in subdirectories:
    subdirectory_path = os.path.join(directory_path, subdirectory)

    if os.path.isdir(subdirectory_path):
        output_subdir = os.path.join(output_directory, subdirectory)
        os.makedirs(output_subdir, exist_ok=True)

        filenames = sorted(os.listdir(subdirectory_path))  # Sort the filenames
        for filename in filenames:
            if filename.endswith('.gz'):
                file_path = os.path.join(subdirectory_path, filename)

                # Print the file being processed
                print(f"Processing file: {file_path}")
                file_start_time = time.time()

                # Initialize lists to hold the extracted data for the current file
                ids, dois, publication_years, author_counts = [], [], [], []
                references_data = []
                no_doi_count = 0  # Counter for lines without DOI in the current file
                total_lines = 0  # Counter for the total lines in the file

                # Read the file and extract information
                try:
                    with gzip.open(file_path, 'rt') as f:
                        for line in f:
                            total_lines += 1
                            openalex_id, doi, publication_year, references, author_count = extract_info(line)
                            ids.append(openalex_id)
                            dois.append(doi)
                            publication_years.append(publication_year)
                            author_counts.append(author_count)

                            if doi == 'DOI not available':
                                no_doi_count += 1

                            for ref in references:
                                references_data.append({'citing': openalex_id, 'cited': ref})
                except Exception as e:
                    error_message = f"Error processing file {file_path}: {e}"
                    print(error_message)
                    errors.append(error_message)
                    continue

                # Update total counts
                total_no_doi_count += no_doi_count
                total_lines_processed += len(ids)

                # Create DataFrames from the extracted data
                main_df = pd.DataFrame({
                    'openalex_id': ids,
                    'DOI': dois,
                    'publication_year': publication_years,
                    'author_count': author_counts
                })

                references_df = pd.DataFrame(references_data)

                # Print the number of records extracted
                print(f"Extracted {len(main_df)} records from {filename}")

                # Verify and print the checks
                file_end_time = time.time()
                file_processing_time = file_end_time - file_start_time
                percent_no_doi = (no_doi_count / total_lines) * 100 if total_lines else 0

                print(f"Total lines in original file: {total_lines}")
                print(f"Number of openalex_id: {len(ids)}")
                print(f"Number of records extracted: {len(main_df)}")
                print(f"Time taken to process {filename}: {file_processing_time:.2f} seconds")
                print(f"Percentage of lines without a valid DOI: {percent_no_doi:.2f}%")

                # Save the DataFrames to Parquet files
                try:
                    main_output_file_path = os.path.join(output_subdir, f"{filename.replace('.gz', '')}_main.parquet")
                    references_output_file_path = os.path.join(output_subdir, f"{filename.replace('.gz', '')}_references.parquet")
                    main_df.to_parquet(main_output_file_path, index=False, engine='pyarrow')
                    references_df.to_parquet(references_output_file_path, index=False, engine='pyarrow')
                    print(f"Saved Parquet files for {filename}")
                except Exception as e:
                    error_message = f"Error saving Parquet files for {file_path}: {e}"
                    print(error_message)
                    errors.append(error_message)

# Calculate the total time taken
end_time = time.time()
processing_time = end_time - start_time

# Calculate the total percentage of lines without a valid DOI
percent_no_doi_total = (total_no_doi_count / total_lines_processed) * 100 if total_lines_processed else 0

# Print the overall statistics
print(f"Total number of lines without a valid DOI: {total_no_doi_count}")
print(f"Total percentage of lines without a valid DOI: {percent_no_doi_total:.2f}%")
print(f"Total number of works processed: {total_lines_processed}")
print(f"Total time taken to process the data: {processing_time:.2f} seconds")

# Write the log file
with open(log_file_path, 'w') as log_file:
    if errors:
        log_file.write("Errors encountered during processing:\n")
        for error in errors:
            log_file.write(f"{error}\n")
    else:
        log_file.write("All files processed successfully without errors.")
