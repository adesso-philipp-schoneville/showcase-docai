# Document Initializer

# ZIP archives of code
data "archive_file" "document_showcase" {
  type        = "zip"
  source_dir  = "../cloud_functions/document_showcase"
  output_path = "../cf_archives/document_showcase.zip"
}

# Upload the function source code to the Cloud Storage bucket
resource "google_storage_bucket_object" "document_showcase" {
  name   = "input-function.${data.archive_file.document_showcase.output_md5}.zip"
  bucket = google_storage_bucket.function_archives.name
  source = data.archive_file.document_showcase.output_path
}

# Cloud Function - Document Initializer
resource "google_cloudfunctions_function" "document_showcase" {
  name                  = "document-showcase"
  description           = "Processes scanned documents in PDF format"
  runtime               = "python310"
  entry_point           = "document_showcase"
  source_archive_bucket = google_storage_bucket.function_archives.name
  source_archive_object = google_storage_bucket_object.document_showcase.name

  # Set the Cloud Function environment variables
  environment_variables = {
    CDS_ID = var.cds_processor_id
    CDE_KFZ_FORMULAR = google_document_ai_processor.cde_formular_formular.id
    CDE_ANSCHREIBEN = google_document_ai_processor.cde_formular_anschreiben.id
    LOCATION = var.docai_location
    FIRESTORE_COLLECTION = var.firestore_collection
  }

  # Set the Cloud Function trigger to execute when a PDF file is uploaded to the input bucket
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.input.name
  }
}

# Data Ingestion

# ZIP archives of code
data "archive_file" "showcase_data_ingestion" {
  type        = "zip"
  source_dir  = "../cloud_functions/showcase_data_ingestion"
  output_path = "../cf_archives/showcase_data_ingestion.zip"
}

# Upload the function source code to the Cloud Storage bucket
resource "google_storage_bucket_object" "showcase_data_ingestion" {
  name   = "input-function.${data.archive_file.showcase_data_ingestion.output_md5}.zip"
  bucket = google_storage_bucket.function_archives.name
  source = data.archive_file.showcase_data_ingestion.output_path
}

# Cloud Function - Data Ingestion
resource "google_cloudfunctions_function" "showcase_data_ingestion" {
  name                  = "showcase-data-ingestion"
  description           = "Ingests sample data into Firestore"
  runtime               = "python310"
  entry_point           = "showcase_data_ingestion"
  trigger_http          = true
  source_archive_bucket = google_storage_bucket.function_archives.name
  source_archive_object = google_storage_bucket_object.showcase_data_ingestion.name

  # Set the Cloud Function environment variables
  environment_variables = {
    LOCATION = var.docai_location
    FIRESTORE_COLLECTION = var.firestore_collection
  }
}

output "cf_data_ingestion_endpoint" {
  value = google_cloudfunctions_function.showcase_data_ingestion.https_trigger_url
}