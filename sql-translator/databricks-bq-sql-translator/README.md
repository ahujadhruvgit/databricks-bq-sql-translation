
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

**Prerequisites:**
- A Google Cloud Project with billing enabled.
- `gcloud` command-line tool installed and authenticated (`gcloud auth login`, `gcloud config set project YOUR_PROJECT_ID`).
- Python (e.g., 3.9, 3.10, or 3.11 - check `main.py` for specific library compatibility if issues arise, but standard versions should work).

**1. Configure Variables in `main.py`:**

Before deploying, it's crucial to update the placeholder variables at the top of `sql-translator/databricks-bq-sql-translator/main.py` with your specific GCP project and GCS bucket details:


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

**4. Grant IAM Permissions to the Cloud Function's Service Account:**

When you deploy the Cloud Function, it will use a runtime service account. This service account needs the following IAM roles/permissions in your project to operate correctly:

*   **For reading input SQL files and RAG context files from GCS:**
    *   `roles/storage.objectViewer` (Storage Object Viewer) on the buckets/objects specified in `CONTEXT_FILES_GCS_PATHS` and where your input SQL files reside.
*   **For writing translated SQL files to GCS:**
    *   `roles/storage.objectCreator` (Storage Object Creator) on the bucket specified in `BQ_SQL_OUTPUT_BUCKET`.
*   **For Vertex AI RAG Corpus and Model Interaction:**
    *   `roles/aiplatform.user` (Vertex AI User) - This provides broad access. For more fine-grained control, you might need specific permissions like `aiplatform.corpora.get`, `aiplatform.corpora.list`, `aiplatform.corpora.create`, `aiplatform.ragResources.importFiles`, `aiplatform.endpoints.predict` (for the embedding model and the Gemini model).
    *   If the RAG Corpus is managed by Vertex AI and uses its own service agent for data access, ensure that service agent also has necessary permissions (e.g., to read from GCS).
*   **For BigQuery Dry Run:**
    *   `roles/bigquery.jobUser` (BigQuery Job User) - This allows creating and running query jobs (including dry runs).
*   **For Logging (usually granted by default):**
    *   `roles/logging.logWriter` (Logs Writer)

You can grant these roles to the Cloud Function's service account (usually `YOUR_PROJECT_ID@appspot.gserviceaccount.com` for 1st gen functions, or a custom one if specified during deployment for 2nd gen) via the IAM page in the Google Cloud Console.

**5. Deploy the Cloud Function:**

Navigate to the `sql-translator/databricks-bq-sql-translator` directory in your terminal.

Here's an example deployment command for a 2nd Generation Cloud Function (recommended):

```bash
gcloud functions deploy translate-databricks-to-bq-sql \
  --gen2 \
  --runtime python311 # Or python39, python310, etc.
  --region YOUR_GCP_REGION # e.g., us-central1
  --source . \
  --entry-point translate_sql \
  --trigger-http \
  --allow-unauthenticated # Remove this flag for a private function
  # --service-account YOUR_FUNCTION_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com # Optional: if not using default
  # --timeout 600s # Optional: increase timeout if processing large files or many files in a batch
  # --memory 1024MB # Optional: adjust memory as needed
```

**Notes on Deployment:**
*   Replace `YOUR_GCP_REGION` with your desired region.
*   Adjust `--runtime` based on your Python version preference (e.g., `python39`, `python310`, `python311`).
*   The `--allow-unauthenticated` flag makes the function public. Remove it to require authentication (see "Authenticating Requests" section above). If removed, ensure the invoker has the `Cloud Functions Invoker` role.
*   If you are using a non-default service account for your function, specify it with `--service-account`. Ensure this service account has the IAM permissions listed above.

## Usage

Once the Cloud Function is deployed, you can invoke it with an HTTP POST request. You can find your `$FUNCTION_URL` in the Google Cloud Console after deployment, or by using the command `gcloud functions describe YOUR_FUNCTION_NAME --region YOUR_REGION --format='value(https_trigger.url)'`.

**Example cURL Request:**

```bash
# 1. Set your function URL (replace with your actual URL)
FUNCTION_URL="YOUR_FUNCTION_TRIGGER_URL"

# 2. Prepare your JSON payload directly
# Option A: Perform dry run (default behavior if "perform_dry_run" is omitted or true)
curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d '{
        "databricks_sql_gcs_paths": ["gs://your-input-sql-bucket/query1.sql", "gs://your-input-sql-bucket/query2.sql"],
        "perform_dry_run": true
      }'

# Option B: Skip dry run
curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d '{
        "databricks_sql_gcs_paths": ["gs://your-input-sql-bucket/another_query.sql"],
        "perform_dry_run": false
      }'
```

**Note:** The `perform_dry_run` field in the JSON payload is optional and defaults to `true` if not provided. For invoking non-public functions, you will also need to provide an authentication token (see next section).

### Authenticating Requests (for non-public functions)

If your Cloud Function is not configured to allow unauthenticated invocations, you'll need to provide an OIDC identity token in the `Authorization` header of your request.

You can obtain a token using the `gcloud` command-line tool:

```bash
# Ensure you are authenticated with gcloud:
# gcloud auth login
# gcloud config set project YOUR_PROJECT_ID

# Get the identity token for the currently authenticated user or service account:
TOKEN=$(gcloud auth print-identity-token)

# Then, include it in your curl request:
curl -X POST "$FUNCTION_URL" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "databricks_sql_gcs_paths": ["gs://your-input-sql-bucket/query1.sql"],
        "perform_dry_run": true
      }'
```
Make sure the account used by `gcloud auth print-identity-token` (your user account or a service account if you're running this from a script/CI/CD) has the `Cloud Functions Invoker` IAM role for the deployed function.

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
