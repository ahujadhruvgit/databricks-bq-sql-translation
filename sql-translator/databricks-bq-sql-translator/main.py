import functions_framework
from vertexai import rag
from vertexai.generative_models import GenerativeModel, Tool
import vertexai
from google.cloud import storage
import os
import datetime
import json
import logging

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

    if not request_json or "databricks_sql_gcs_path" not in request_json:
        msg = "Missing 'databricks_sql_gcs_path' in request."
        logger.error(msg)
        return json.dumps({"error": msg}), 400

    databricks_sql_gcs_path = request_json["databricks_sql_gcs_path"]
    logger.info(f"Translating SQL from GCS path: {databricks_sql_gcs_path}")

    try:
        path_parts = databricks_sql_gcs_path.replace("gs://", "").split("/", 1)
        if len(path_parts) < 2:
            raise ValueError(f"Invalid GCS path: {databricks_sql_gcs_path}")
        input_bucket_name, input_blob_name = path_parts

        input_bucket = storage_client.bucket(input_bucket_name)
        input_blob = input_bucket.blob(input_blob_name)

        if not input_blob.exists():
            raise FileNotFoundError(f"Input file not found: {databricks_sql_gcs_path}")

        databricks_sql_content = input_blob.download_as_text()
        logger.info(f"Read SQL content (length: {len(databricks_sql_content)} chars).")

        _initialize_rag_resources() 
        
        if not rag_retrieval_tool_global:
            logger.error("RAG retrieval tool was not initialized. Aborting.")
            raise RuntimeError("RAG retrieval tool failed to initialize.")

        gemini_model_name_str = "gemini-2.0-flash-001" 

        rag_model = GenerativeModel(
            model_name=gemini_model_name_str, 
            tools=[rag_retrieval_tool_global] 
        )
        
        logger.info(f"Gemini model ({gemini_model_name_str}) with RAG tool initialized for generation.")


        prompt = f"Translate this Databricks SQL to BigQuery SQL, ensuring all functions, data types, and syntax are compatible with BigQuery: {databricks_sql_content}"
        logger.info(f"Sending prompt for translation (first 150 chars): '{prompt[:150]}...'")
        
        response = rag_model.generate_content(prompt)
        
        if hasattr(response, 'text'):
            bq_sql_content = response.text
        else:
            logger.warning(f"Response object does not have a 'text' attribute. Full response: {response}")
            
            try:
                bq_sql_content = "".join(part.text for part in response.candidates[0].content.parts)
            except Exception:
                logger.error("Could not extract text from model response. Defaulting to empty string.")
                bq_sql_content = f"Error: Could not extract text from model response. Raw response: {response}"


        logger.info("SQL translation received.")

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        original_filename = os.path.basename(input_blob_name)
        output_filename_base = os.path.splitext(original_filename)[0]
        output_blob_name = f"translated_sql/{output_filename_base}_{timestamp}_bq.sql"

        output_bucket = storage_client.bucket(BQ_SQL_OUTPUT_BUCKET)
        output_blob = output_bucket.blob(output_blob_name)

        logger.info(f"Uploading translated SQL to gs://{BQ_SQL_OUTPUT_BUCKET}/{output_blob_name}")
        output_blob.upload_from_string(bq_sql_content, content_type="text/plain")
        translated_gcs_path = f"gs://{BQ_SQL_OUTPUT_BUCKET}/{output_blob_name}"
        logger.info(f"Translated SQL uploaded to: {translated_gcs_path}")

        return json.dumps({"translated_gcs_path": translated_gcs_path}), 200

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}", exc_info=True)
        return json.dumps({"error": str(e)}), 404
    except ValueError as e:
        logger.error(f"Input error: {e}", exc_info=True)
        return json.dumps({"error": str(e)}), 400
    except RuntimeError as e: 
        logger.error(f"Runtime error: {e}", exc_info=True)
        return json.dumps({"error": str(e)}), 500
    except Exception as e:
        logger.critical(f"Unhandled error in SQL translation: {type(e).__name__}: {e}", exc_info=True)
        return json.dumps({"error": "Internal Server Error."}), 500