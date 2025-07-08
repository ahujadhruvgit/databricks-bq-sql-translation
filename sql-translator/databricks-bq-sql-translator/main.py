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
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "{PROJECT_ID}"
LOCATION = "us-central1"

if not PROJECT_ID:
    logger.critical("GCP_PROJECT environment variable not set. This is a critical configuration error.")
    raise ValueError("GCP_PROJECT environment variable is not set. Cannot proceed.")

# Display name of your RAG corpus
RAG_CORPUS_DISPLAY_NAME = "test_rag_corpus" 

#These files will be used to create RAG Corpus
#TODO : Upload this file to GCS and pass the path below.
CONTEXT_FILES_GCS_PATHS = [
    "gs://Databricks SQL to BigQuery SQL Migration.pdf"
]
#Outpus GCS Bucket name
BQ_SQL_OUTPUT_BUCKET = "{GCS_OUTPUT_BUCKET_PATH}"

#Initialize Vertex AI Project
logger.info(f"Attempting to initialize Vertex AI for project: {PROJECT_ID}, location: {LOCATION}")
try:
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    logger.info("Vertex AI initialized successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to initialize Vertex AI SDK. Error: {type(e).__name__}: {e}", exc_info=True)
    raise

#Initialize GCS Client
logger.info("Attempting to initialize Google Cloud Storage client.")
try:
    storage_client = storage.Client()
    logger.info("Google Cloud Storage client initialized successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to initialize GCS client. Error: {type(e).__name__}: {e}", exc_info=True)
    raise

# Initialize BigQuery Client
logger.info("Attempting to initialize Google BigQuery client.")
try:
    bigquery_client = bigquery.Client()
    logger.info("Google BigQuery client initialized successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to initialize BigQuery client. Error: {type(e).__name__}: {e}", exc_info=True)
    raise

rag_corpus_global = None
rag_retrieval_tool_global = None

#Initialize and create RAG Corpus object
def _initialize_rag_resources():
    global rag_corpus_global, rag_retrieval_tool_global 

    if rag_corpus_global and rag_retrieval_tool_global:
        logger.info("RAG resources already initialized. Skipping re-initialization.")
        return

    logger.info("Initializing RAG resources...")
    try:
        logger.info(f"Listing RAG corpora to find '{RAG_CORPUS_DISPLAY_NAME}'...")
        all_corpora_response = rag.list_corpora()
        
        found_corpus = None
        iterable_corpora = all_corpora_response 
        
        if not isinstance(all_corpora_response, list) and hasattr(all_corpora_response, 'corpora'):
             iterable_corpora = all_corpora_response.corpora
        elif not isinstance(all_corpora_response, list): 
            logger.warning(f"list_corpora returned type {type(all_corpora_response)}, not a list or expected wrapper. Assuming it's empty or not iterable as expected.")
            iterable_corpora = []


        for corpus_item in iterable_corpora:
            if hasattr(corpus_item, 'display_name') and corpus_item.display_name == RAG_CORPUS_DISPLAY_NAME:
                found_corpus = corpus_item
                break
        
        if found_corpus:
            rag_corpus_global = found_corpus
            logger.info(f"Found existing RAG corpus: {rag_corpus_global.name} (Display Name: {rag_corpus_global.display_name}).")
        else:
            logger.warning(f"RAG corpus '{RAG_CORPUS_DISPLAY_NAME}' not found. Creating new corpus...")
            
            embedding_model_config = rag.RagEmbeddingModelConfig(
                vertex_prediction_endpoint=rag.VertexPredictionEndpoint(
                    publisher_model="publishers/google/models/text-embedding-005"
                )
            )
            logger.info(f"Configured RAG embedding model: {embedding_model_config.vertex_prediction_endpoint.publisher_model}.")
            
            rag_corpus_global = rag.create_corpus(
                display_name=RAG_CORPUS_DISPLAY_NAME,
                backend_config=rag.RagVectorDbConfig(
                    rag_embedding_model_config=embedding_model_config
                ),
            )
            logger.info(f"Created new RAG corpus: {rag_corpus_global.name}. Importing files...")

            import_response = rag.import_files(
                rag_corpus_global.name,
                CONTEXT_FILES_GCS_PATHS,
                transformation_config=rag.TransformationConfig(
                    chunking_config=rag.ChunkingConfig(
                        chunk_size=512,
                        chunk_overlap=100,
                    ),
                ),
            )
            if hasattr(import_response, 'result') and callable(import_response.result):
                 logger.info("Waiting for import_files LRO to complete...")
                 import_response.result() 
                 logger.info("File import LRO completed.")
            else:
                logger.info(f"File import for new RAG corpus initiated/completed. Response: {import_response}")


    except Exception as e:
        logger.error(f"CRITICAL ERROR during RAG resource initialization: {type(e).__name__}: {e}", exc_info=True)
        raise 

    logger.info("Creating RAG retrieval tool...")
    rag_retrieval_config = rag.RagRetrievalConfig(
        top_k=3,
    )
    
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

    # Get the perform_dry_run flag, default to True if not provided
    perform_dry_run = request_json.get("perform_dry_run", True)
    if not isinstance(perform_dry_run, bool):
        msg = "'perform_dry_run' must be a boolean (true or false)."
        logger.error(msg)
        return json.dumps({"error": msg}), 400

    logger.info(f"Batch processing requested. Perform dry run: {perform_dry_run}")

    results = []

    # Initialize RAG resources once if not already done
    try:
        _initialize_rag_resources()
        if not rag_retrieval_tool_global:
            logger.error("RAG retrieval tool was not initialized. Aborting batch processing.")
            # For batch, we might want to return a general error or partial results.
            # Here, failing early if RAG isn't ready.
            return json.dumps({"error": "RAG retrieval tool failed to initialize. Cannot process batch."}), 500

        gemini_model_name_str = "gemini-2.0-flash-001"
        rag_model = GenerativeModel(
            model_name=gemini_model_name_str,
            tools=[rag_retrieval_tool_global]
        )
        logger.info(f"Gemini model ({gemini_model_name_str}) with RAG tool initialized for generation.")

    except Exception as e:
        logger.critical(f"FATAL: Failed during common resource initialization (RAG/Model). Error: {type(e).__name__}: {e}", exc_info=True)
        return json.dumps({"error": f"Failed to initialize resources: {str(e)}"}), 500


    for databricks_sql_gcs_path in databricks_sql_gcs_paths:
        logger.info(f"Processing SQL from GCS path: {databricks_sql_gcs_path}")
        file_result = {"input_gcs_path": databricks_sql_gcs_path}

        try:
            path_parts = databricks_sql_gcs_path.replace("gs://", "").split("/", 1)
            if len(path_parts) < 2:
                raise ValueError(f"Invalid GCS path format: {databricks_sql_gcs_path}")
            input_bucket_name, input_blob_name = path_parts

            input_bucket = storage_client.bucket(input_bucket_name)
            input_blob = input_bucket.blob(input_blob_name)

            if not input_blob.exists():
                raise FileNotFoundError(f"Input file not found: {databricks_sql_gcs_path}")

            databricks_sql_content = input_blob.download_as_text()
            logger.info(f"Read SQL content (length: {len(databricks_sql_content)} chars) for {databricks_sql_gcs_path}.")

            prompt = f"""\
Translate the following Databricks SQL to BigQuery SQL.
Ensure all functions, data types, and syntax are compatible with BigQuery.
Return ONLY the translated BigQuery SQL query, enclosed in triple backticks with the language identifier 'sql'.
For example:
```sql
SELECT * FROM my_table;
```

Databricks SQL to translate:
{databricks_sql_content}
"""
            logger.info(f"Sending prompt for {databricks_sql_gcs_path} (first 250 chars): '{prompt[:250].replacechr(10, ' ')}...'")

            response = rag_model.generate_content(prompt)

            bq_sql_content_raw = ""
            if hasattr(response, 'text'):
                bq_sql_content_raw = response.text
            else:
                logger.warning(f"Response object for {databricks_sql_gcs_path} does not have a 'text' attribute. Full response: {response}")
                try:
                    bq_sql_content_raw = "".join(part.text for part in response.candidates[0].content.parts)
                except Exception:
                    logger.error(f"Could not extract text from model response for {databricks_sql_gcs_path}. Defaulting to empty string.")
                    # bq_sql_content_raw remains empty

            logger.info(f"Raw SQL translation for {databricks_sql_gcs_path} (first 150 chars): '{bq_sql_content_raw[:150].replacechr(10, ' ')}...'")

            extracted_sql = bq_sql_content_raw
            sql_match = re.search(r"```sql\s*(.*?)\s*```", bq_sql_content_raw, re.DOTALL | re.IGNORECASE)
            if sql_match:
                extracted_sql = sql_match.group(1).strip()
                logger.info(f"Extracted SQL for {databricks_sql_gcs_path} (first 150 chars): '{extracted_sql[:150].replacechr(10, ' ')}...'")
            else:
                logger.warning(f"SQL delimiter ```sql ... ``` not found in model response for {databricks_sql_gcs_path}. Using entire response.")

            if not extracted_sql:
                logger.error(f"Extracted SQL is empty for {databricks_sql_gcs_path}.")
                # Fall through, dry run will be skipped or fail.
            
            file_result["translated_sql_raw_output"] = bq_sql_content_raw # For debugging if needed
            file_result["translated_sql_extracted"] = extracted_sql

            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            original_filename = os.path.basename(input_blob_name)
            output_filename_base = os.path.splitext(original_filename)[0]
            output_blob_name = f"translated_sql/{output_filename_base}_{timestamp}_bq.sql"

            output_bucket = storage_client.bucket(BQ_SQL_OUTPUT_BUCKET)
            output_blob = output_bucket.blob(output_blob_name)

            logger.info(f"Uploading extracted SQL for {databricks_sql_gcs_path} to gs://{BQ_SQL_OUTPUT_BUCKET}/{output_blob_name}")
            output_blob.upload_from_string(extracted_sql, content_type="text/plain")
            translated_gcs_path = f"gs://{BQ_SQL_OUTPUT_BUCKET}/{output_blob_name}"
            file_result["translated_gcs_path"] = translated_gcs_path
            logger.info(f"Extracted SQL for {databricks_sql_gcs_path} uploaded to: {translated_gcs_path}")

            dry_run_results = {}
            if perform_dry_run:
                try:
                    logger.info(f"Performing BigQuery dry run on extracted SQL for {databricks_sql_gcs_path} (length: {len(extracted_sql)} chars)...")
                    if not extracted_sql.strip():
                        logger.warning(f"Extracted SQL for {databricks_sql_gcs_path} is empty or whitespace. Skipping dry run.")
                        dry_run_results["status"] = "SKIPPED_EMPTY_SQL"
                        dry_run_results["reason"] = "Extracted SQL was empty."
                    else:
                        job_config = bigquery.QueryJobConfig(dry_run=True, use_legacy_sql=False)
                        dry_run_job = bigquery_client.query(extracted_sql, job_config=job_config)
                        dry_run_results["status"] = "SUCCESS"
                        dry_run_results["total_bytes_processed"] = dry_run_job.total_bytes_processed
                        logger.info(f"Dry run for {databricks_sql_gcs_path} successful. Bytes processed: {dry_run_job.total_bytes_processed}")
                except Exception as e_dry_run:
                    logger.error(f"BigQuery dry run for {databricks_sql_gcs_path} failed: {type(e_dry_run).__name__}: {e_dry_run}", exc_info=True)
                    dry_run_results["status"] = "FAILURE"
                    dry_run_results["error_message"] = str(e_dry_run)
            else:
                logger.info(f"Dry run skipped for {databricks_sql_gcs_path} as per user request.")
                dry_run_results["status"] = "SKIPPED_BY_USER_REQUEST"

            file_result["dry_run_results"] = dry_run_results

        except FileNotFoundError as e_file:
            logger.error(f"File not found for {databricks_sql_gcs_path}: {e_file}", exc_info=True)
            file_result["error"] = str(e_file)
            file_result["status"] = "ERROR_FILE_NOT_FOUND"
        except ValueError as e_value:
            logger.error(f"Input error for {databricks_sql_gcs_path}: {e_value}", exc_info=True)
            file_result["error"] = str(e_value)
            file_result["status"] = "ERROR_INVALID_INPUT"
        except RuntimeError as e_runtime:
            logger.error(f"Runtime error processing {databricks_sql_gcs_path}: {e_runtime}", exc_info=True)
            file_result["error"] = str(e_runtime)
            file_result["status"] = "ERROR_RUNTIME"
        except Exception as e_general:
            logger.critical(f"Unhandled error processing {databricks_sql_gcs_path}: {type(e_general).__name__}: {e_general}", exc_info=True)
            file_result["error"] = f"An unexpected error occurred: {str(e_general)}"
            file_result["status"] = "ERROR_UNHANDLED"

        results.append(file_result)

    logger.info(f"Batch processing complete. Processed {len(databricks_sql_gcs_paths)} file(s).")
    return json.dumps(results), 200