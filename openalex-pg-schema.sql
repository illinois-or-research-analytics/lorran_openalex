-- based on https://github.com/ourresearch/openalex-documentation-scripts/blob/main/openalex-pg-schema.sql

CREATE SCHEMA openalex;

CREATE TABLE openalex.works (
    id text NOT NULL PRIMARY KEY,
    doi text,
    publication_year integer,
    author_count integer
);

CREATE TABLE openalex.works_referenced_works (
    work_id text,
    referenced_work_id text
);
