#this code is based on https://github.com/ourresearch/openalex-documentation-scripts/blob/main/flatten-openalex-jsonl.py

import csv
import glob
import gzip
import json
import os

SNAPSHOT_DIR = 'openalex-snapshot'
CSV_DIR = 'csv-files'

if not os.path.exists(CSV_DIR):
    os.mkdir(CSV_DIR)

#FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

csv_files = {
    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'id', 'doi', 'publication_year', 'author_count'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
    },
}

def flatten_works():
    file_spec = csv_files['works']

    with gzip.open(file_spec['works']['name'], 'wt', encoding='utf-8') as works_csv, \
            gzip.open(file_spec['referenced_works']['name'], 'wt', encoding='utf-8') as referenced_works_csv:

        works_writer = csv.DictWriter(works_csv, fieldnames=file_spec['works']['columns'], extrasaction='ignore')
        works_writer.writeheader()

        referenced_works_writer = csv.DictWriter(referenced_works_csv, fieldnames=file_spec['referenced_works']['columns'])
        referenced_works_writer.writeheader()

        files_done = 0
        for date_folder in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'works', 'updated_date=*')):
            for jsonl_file_name in glob.glob(os.path.join(date_folder, '*.gz')):
                print(jsonl_file_name)
                with gzip.open(jsonl_file_name, 'r') as works_jsonl:
                    for work_json in works_jsonl:
                        if not work_json.strip():
                            continue

                        work = json.loads(work_json)

                        if not (work_id := work.get('id')):
                            continue

                        # works
                        work_data = {
                            'id': work.get('id'),
                            'doi': work.get('doi'),
                            'publication_year': work.get('publication_year'),
                            'author_count': len(work.get('authorships', []))
                        }
                        works_writer.writerow(work_data)

                        # referenced_works
                        for referenced_work in work.get('referenced_works', []):
                            if referenced_work:
                                referenced_works_writer.writerow({
                                    'work_id': work_id,
                                    'referenced_work_id': referenced_work
                                })

                # files_done += 1
                # if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                #     break

if __name__ == '__main__':
    flatten_works()
