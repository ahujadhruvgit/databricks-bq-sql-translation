import functions_framework
from vertexai import rag
from vertexai.generative_models import GenerativeModel, Tool
import vertexai
from google.cloud import storage
from google.cloud import bigquery
import os
import datetime
import json
import logging
import yaml
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration Loading ---
PROJECT_ID = None
LOCATION = None
GEMINI_MODEL_NAME_STR = None
RAG_RESOURCE_ID = None
BQ_SQL_OUTPUT_BUCKET = None

try:
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    PROJECT_ID = config.get("project_id")
    LOCATION = config.get("location")
    GEMINI_MODEL_NAME_STR = config.get("gemini_model_name_str")
    RAG_RESOURCE_ID = config.get("rag_resource_id") 
    BQ_SQL_OUTPUT_BUCKET = config.get("bq_sql_output_bucket")

    logger.info(f"PROJECT_ID: {PROJECT_ID}")
    logger.info(f"LOCATION: {LOCATION}")
    logger.info(f"GEMINI_MODEL_NAME_STR: {GEMINI_MODEL_NAME_STR}")
    logger.info(f"RAG_RESOURCE_ID (Corpus Name): {RAG_RESOURCE_ID}") 
    logger.info(f"BQ_SQL_OUTPUT_BUCKET: {BQ_SQL_OUTPUT_BUCKET}")

except FileNotFoundError:
    logger.critical("Error: config.yaml not found. Please make sure the file exists.")
    raise FileNotFoundError("config.yaml not found. Cannot proceed.")
except yaml.YAMLError as exc:
    logger.critical(f"Error parsing YAML file: {exc}")
    raise yaml.YAMLError(f"Error parsing YAML file: {exc}")

if not PROJECT_ID:
    logger.critical("PROJECT_ID not set from config. This is a critical configuration error.")
    raise ValueError("PROJECT_ID is not set. Cannot proceed.")
if not RAG_RESOURCE_ID:
    logger.critical("RAG_RESOURCE_ID not set from config. This is a critical configuration error.")
    raise ValueError("RAG_RESOURCE_ID is not set. Cannot proceed.")


# --- Global Client Initializations ---
logger.info(f"Attempting to initialize Vertex AI for project: {PROJECT_ID}, location: {LOCATION}")
try:
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    logger.info("Vertex AI initialized successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to initialize Vertex AI SDK. Error: {type(e).__name__}: {e}", exc_info=True)
    raise

logger.info("Attempting to initialize Google Cloud Storage client.")
try:
    storage_client = storage.Client()
    logger.info("Google Cloud Storage client initialized successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to initialize GCS client. Error: {type(e).__name__}: {e}", exc_info=True)
    raise

logger.info("Attempting to initialize Google BigQuery client.")
try:
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    logger.info("Google BigQuery client initialized successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to initialize BigQuery client. Error: {type(e).__name__}: {e}", exc_info=True)
    raise

rag_corpus_global = None
rag_retrieval_tool_global = None
gemini_rag_model_global = None 

# --- RAG Resource Initialization Function ---
def _initialize_rag_resources():
    global rag_corpus_global, rag_retrieval_tool_global, gemini_rag_model_global

    if rag_corpus_global and rag_retrieval_tool_global and gemini_rag_model_global:
        logger.info("RAG resources and Gemini model already initialized. Skipping re-initialization.")
        return

    logger.info("Initializing RAG resources and Gemini model...")
    try:
        # Assuming RAG_RESOURCE_ID is the full corpus NAME, e.g., "projects/PROJECT_NUMBER/locations/LOCATION/ragCorpora/CORPUS_ID"
        logger.info(f"Using RAG corpus: {RAG_RESOURCE_ID}")
        rag_corpus_global = rag.RagCorpus(name=RAG_RESOURCE_ID) 

        rag_retrieval_config = rag.RagRetrievalConfig(top_k=3)
        
        rag_retrieval_tool_global = Tool.from_retrieval(
            retrieval=rag.Retrieval(
                source=rag.VertexRagStore(
                    rag_resources=[
                        rag.RagResource(
                            rag_corpus=rag_corpus_global.name,
                        )
                    ],
                    rag_retrieval_config=rag_retrieval_config,
                ),
            )
        )
        logger.info("RAG retrieval tool initialized successfully.")

        gemini_rag_model_global = GenerativeModel(
            model_name=GEMINI_MODEL_NAME_STR,
            tools=[rag_retrieval_tool_global]
        )
        logger.info(f"Gemini model ({GEMINI_MODEL_NAME_STR}) with RAG tool initialized successfully.")

    except Exception as e:
        logger.critical(f"CRITICAL ERROR during RAG resource or Gemini model initialization: {type(e).__name__}: {e}", exc_info=True)
        raise

# Ensure RAG resources are initialized once when the function container starts
try:
    _initialize_rag_resources()
except Exception:
    logger.critical("Pre-flight RAG resource initialization failed. Function will not be able to process requests.")
    exit(1) 

@functions_framework.http
def translate_sql(request):
    logger.info("Received request to translate SQL.")
    request_json = request.get_json(silent=True)

    if not request_json or "databricks_sql_gcs_paths" not in request_json:
        msg = "Missing 'databricks_sql_gcs_paths' (list of GCS file paths) in request."
        logger.error(msg)
        return json.dumps({"error": msg}), 400

    databricks_sql_gcs_paths = request_json["databricks_sql_gcs_paths"]
    if not isinstance(databricks_sql_gcs_paths, list) or not all(isinstance(p, str) for p in databricks_sql_gcs_paths):
        msg = "'databricks_sql_gcs_paths' must be a list of strings."
        logger.error(msg)
        return json.dumps({"error": msg}), 400
    
    if not databricks_sql_gcs_paths:
        msg = "'databricks_sql_gcs_paths' list cannot be empty."
        logger.error(msg)
        return json.dumps({"error": msg}), 400

    perform_dry_run = request_json.get("perform_dry_run", True)
    if not isinstance(perform_dry_run, bool):
        msg = "'perform_dry_run' must be a boolean (true or false)."
        logger.error(msg)
        return json.dumps({"error": msg}), 400
    
    logger.info(f"Batch processing requested. Perform dry run: {perform_dry_run}")

    results = []

    if not gemini_rag_model_global:
        logger.critical("Gemini RAG model not initialized. This indicates a critical startup failure.")
        return json.dumps({"error": "Server internal error: Model not initialized."}), 500

    for databricks_sql_gcs_p in databricks_sql_gcs_paths:
        logger.info(f"Processing SQL from GCS path: {databricks_sql_gcs_p}")
        file_result = {"input_gcs_path": databricks_sql_gcs_p}

        try:
            path_parts = databricks_sql_gcs_p.replace("gs://", "").split("/", 1)
            if len(path_parts) < 2:
                raise ValueError(f"Invalid GCS path format: {databricks_sql_gcs_p}")
            input_bucket_name, input_blob_name = path_parts

            input_bucket = storage_client.bucket(input_bucket_name)
            input_blob = input_bucket.blob(input_blob_name)

            if not input_blob.exists():
                raise FileNotFoundError(f"Input file not found: {databricks_sql_gcs_p}")

            databricks_sql_content = input_blob.download_as_text()
            logger.info(f"Read SQL content (length: {len(databricks_sql_content)} chars) for {databricks_sql_gcs_p}.")

            prompt = f"""\
                    Translate the following Databricks SQL to BigQuery SQL.
                    Ensure all functions, data types, and syntax are compatible with BigQuery.
                    Return ONLY the translated BigQuery SQL query, enclosed in triple backticks with the language identifier 'sql'.
                    For example:
                    ```sql
                    SELECT * FROM my_table;
                    Databricks SQL to translate:
                    {databricks_sql_content}
                    """
            truncated_prompt = prompt[:250].replace('\n', ' ')
            logger.info(f"Sending prompt for {databricks_sql_gcs_p} (first 250 chars): '{truncated_prompt}...'")
            response = gemini_rag_model_global.generate_content(prompt)

            bq_sql_content_raw = ""
            if hasattr(response, 'text'):
                bq_sql_content_raw = response.text
            else:
                logger.warning(f"Response object for {databricks_sql_gcs_p} does not have a 'text' attribute. Full response: {response}")
                try:
                    bq_sql_content_raw = "".join(part.text for part in response.candidates[0].content.parts)
                except Exception:
                    logger.error(f"Could not extract text from model response for {databricks_sql_gcs_p}. Defaulting to empty string.")

            raw_sql_truncated = bq_sql_content_raw[:150].replace('\n', ' ')
            logger.info(f"Raw SQL translation for {databricks_sql_gcs_p} (first 150 chars): '{raw_sql_truncated}...'")


            extracted_sql = bq_sql_content_raw
            sql_match = re.search(r"```sql\s*(.*?)\s*```", bq_sql_content_raw, re.DOTALL | re.IGNORECASE)
            if sql_match:
                extracted_sql = sql_match.group(1).strip()
                extracted_sql_truncated = extracted_sql[:150].replace('\n', ' ')
                logger.info(f"Extracted SQL for {databricks_sql_gcs_p} (first 150 chars): '{extracted_sql_truncated}...'")
            else:
                logger.warning(f"SQL delimiter ```sql ... ``` not found in model response for {databricks_sql_gcs_p}. Using entire response.")

            if not extracted_sql:
                logger.info(f"Extracted SQL for {databricks_sql_gcs_p} is empty after extraction. Dry run will be skipped or fail.")
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            original_filename = os.path.basename(input_blob_name)
            output_filename_base = os.path.splitext(original_filename)[0]
            output_blob_name = f"translated_sql/{output_filename_base}_{timestamp}_bq.sql"

            output_bucket = storage_client.bucket(BQ_SQL_OUTPUT_BUCKET)
            output_blob = output_bucket.blob(output_blob_name)

            logger.info(f"Uploading extracted SQL for {databricks_sql_gcs_p} to gs://{BQ_SQL_OUTPUT_BUCKET}/{output_blob_name}")
            output_blob.upload_from_string(extracted_sql, content_type="text/plain")
            translated_gcs_path = f"gs://{BQ_SQL_OUTPUT_BUCKET}/{output_blob_name}"
            file_result["translated_gcs_path"] = translated_gcs_path
            logger.info(f"Extracted SQL for {databricks_sql_gcs_p} uploaded to: {translated_gcs_path}")

            dry_run_results = {}
            if perform_dry_run:
                try:
                    logger.info(f"Performing BigQuery dry run on extracted SQL for {databricks_sql_gcs_p} (length: {len(extracted_sql)} chars)...")
                    if not extracted_sql.strip():
                        logger.warning(f"Extracted SQL for {databricks_sql_gcs_p} is empty or whitespace. Skipping dry run.")
                        dry_run_results["status"] = "SKIPPED_EMPTY_SQL"
                        dry_run_results["reason"] = "Extracted SQL was empty."
                    else:
                        job_config = bigquery.QueryJobConfig(dry_run=True, use_legacy_sql=False)
                        dry_run_job = bigquery_client.query(extracted_sql, job_config=job_config)
                        dry_run_results["status"] = "SUCCESS"
                        dry_run_results["total_bytes_processed"] = dry_run_job.total_bytes_processed
                        logger.info(f"Dry run for {databricks_sql_gcs_p} successful. Bytes processed: {dry_run_job.total_bytes_processed}")
                except Exception as e_dry_run:
                    logger.error(f"BigQuery dry run for {databricks_sql_gcs_p} failed: {type(e_dry_run).__name__}: {e_dry_run}", exc_info=True)
                    dry_run_results["status"] = "FAILURE"
                    dry_run_results["error_message"] = str(e_dry_run)
            else:
                logger.info(f"Dry run skipped for {databricks_sql_gcs_p} as per user request.")
                dry_run_results["status"] = "SKIPPED_BY_USER_REQUEST"

            file_result["dry_run_results"] = dry_run_results

        except FileNotFoundError as e_file:
            logger.error(f"File not found for {databricks_sql_gcs_p}: {e_file}", exc_info=True)
            file_result["error"] = str(e_file)
            file_result["status"] = "ERROR_FILE_NOT_FOUND"
        except ValueError as e_value:
            logger.error(f"Input error for {databricks_sql_gcs_p}: {e_value}", exc_info=True)
            file_result["error"] = str(e_value)
            file_result["status"] = "ERROR_INVALID_INPUT"
        except RuntimeError as e_runtime:
            logger.error(f"Runtime error processing {databricks_sql_gcs_p}: {e_runtime}", exc_info=True)
            file_result["error"] = str(e_runtime)
            file_result["status"] = "ERROR_RUNTIME"
        except Exception as e_general:
            logger.critical(f"Unhandled error processing {databricks_sql_gcs_p}: {type(e_general).__name__}: {e_general}", exc_info=True)
            file_result["error"] = f"An unexpected error occurred: {str(e_general)}"
            file_result["status"] = "ERROR_UNHANDLED"

        results.append(file_result)

        logger.info(f"Batch processing complete. Processed {len(databricks_sql_gcs_paths)} file(s).")
        return json.dumps(results), 200
