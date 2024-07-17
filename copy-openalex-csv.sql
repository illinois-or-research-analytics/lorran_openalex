-- based on https://github.com/ourresearch/openalex-documentation-scripts/blob/main/copy-openalex-csv.sql

-- works
\copy openalex.works (id, doi, publication_year, author_count) from program 'gunzip -c csv-files/works.csv.gz' csv header

-- works_referenced_works
\copy openalex.works_referenced_works (work_id, referenced_work_id) from program 'gunzip -c csv-files/works_referenced_works.csv.gz' csv header
