
# Databricks to BigQuery SQL Translation Accelerator

This repository contains a Google Cloud Function that leverages Vertex AI's Generative Models with Retrieval Augmented Generation (RAG) to translate Databricks SQL queries into BigQuery SQL queries. The function reads SQL content from a specified Google Cloud Storage (GCS) path, performs the translation, and then uploads the translated BigQuery SQL to another GCS bucket.


## Features

- SQL Translation: Translates Databricks SQL syntax to BigQuery SQL syntax. Supports processing multiple files in a single request.
- BigQuery Dry Run: Performs a dry run of the translated SQL query (extracted from the LLM response) against BigQuery to validate syntax and estimate costs. This can be optionally skipped.
- RAG Integration: Utilizes a Vertex AI RAG Corpus for context-aware translation. The LLM is prompted to return translated SQL within ```sql ... ``` delimiters to aid reliable parsing and extraction.
- GCS Integration: Reads input SQL files from GCS and writes the extracted, translated SQL files back to GCS.
- Automatic RAG Corpus Management: Automatically creates and populates a RAG Corpus with specified context files if it doesn't already exist.
- Cloud Function Deployment: Designed to be deployed as a Google Cloud Function triggered by HTTP requests.
- LLM Used : This translation will use gemini-2.0-flash-001 model for translation.

## Architecture Design

![App Screenshot](architecture.png)


## Setup and Deployment

- Configure Project and Storage
Update the placeholder variables in your main.py file with your specific GCP project and GCS bucket details:


```python
PROJECT_ID = "your-gcp-project-id"
LOCATION = "us-central1"

# Display name of your RAG corpus
RAG_CORPUS_DISPLAY_NAME = "test_rag_corpus"

CONTEXT_FILES_GCS_PATHS = [
    "gs://your-rag-context-bucket/Databricks SQL to BigQuery SQL Migration.pdf",
    # Add more context files as needed
]
# Outputs GCS Bucket name
BQ_SQL_OUTPUT_BUCKET = "your-bq-sql-output-bucket"
```
    
- Create GCS Bucket
Make sure the GCS buckets specified in CONTEXT_FILES_GCS_PATHS and BQ_SQL_OUTPUT_BUCKET exist in your project. If not, create them using gsutil:

```bash
gsutil mb gs://your-rag-context-bucket
gsutil mb gs://your-bq-sql-output-bucket
```

- Upload Context Files
Upload the PDF document(s) you want to use for the RAG corpus to the GCS bucket specified in CONTEXT_FILES_GCS_PATHS.

```bash
gsutil cp "path/to/your/Databricks SQL to BigQuery SQL Migration.pdf" gs://your-rag-context-bucket/
```

- Deploy the Cloud Function

## Usage

Once the Cloud Function is deployed, you can invoke it with an HTTP POST request.

```bash
INPUT_SQL_GCS_PATHS='["gs://your-input-sql-bucket/query1.sql", "gs://your-input-sql-bucket/query2.sql"]' # Ensure these files exist in GCS
PERFORM_DRY_RUN=false # Optional: set to true or false

curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d '{
        "databricks_sql_gcs_paths": '"$INPUT_SQL_GCS_PATHS"',
        "perform_dry_run": '$PERFORM_DRY_RUN'
      }'
```

The `perform_dry_run` field in the JSON payload is optional and defaults to `true` if not provided.

- Response
The function will return a JSON array, where each object represents the result for one of the input SQL files:

```json
[
    {
        "input_gcs_path": "gs://your-input-sql-bucket/query1.sql",
        "translated_gcs_path": "gs://your-bq-sql-output-bucket/translated_sql/query1_YYYYMMDDHHMMSS_bq.sql",
        "dry_run_results": {
            "status": "SUCCESS",
            "total_bytes_processed": 1024
    }
    },
    {
        "input_gcs_path": "gs://your-input-sql-bucket/query2.sql",
        "translated_gcs_path": "gs://your-bq-sql-output-bucket/translated_sql/query2_YYYYMMDDHHMMSS_bq.sql",
        "dry_run_results": {
            "status": "FAILURE",
            "error_message": "Syntax error: Unrecognized name: non_existent_table at [1:15]"
    }
    },
    {
        "input_gcs_path": "gs://your-input-sql-bucket/non_existent_query.sql",
        "error": "Input file not found: gs://your-input-sql-bucket/non_existent_query.sql",
    "status": "ERROR_FILE_NOT_FOUND"
    }
]
```
Each item in the response array will contain:
- `input_gcs_path`: The original GCS path of the Databricks SQL file.
- `translated_gcs_path` (on success): The GCS path where the translated BigQuery SQL file has been saved.
- `dry_run_results`: An object containing the status of the BigQuery dry run. Possible statuses include:
    - `SUCCESS`: Dry run was successful. Includes `total_bytes_processed`.
    - `FAILURE`: Dry run failed. Includes `error_message`.
    - `SKIPPED_EMPTY_SQL`: Dry run was skipped because the extracted SQL was empty. Includes `reason`.
    - `SKIPPED_BY_USER_REQUEST`: Dry run was skipped because `perform_dry_run` was set to `false`.
- `error` (on processing failure for that file): A message describing the error.
- `status` (on processing failure for that file): An error code indicating the type of failure (e.g., `ERROR_FILE_NOT_FOUND`, `ERROR_INVALID_INPUT`).
- `translated_sql_raw_output` (optional, for debugging): The raw output from the LLM.
- `translated_sql_extracted` (optional, for debugging): The SQL extracted from the LLM output.
