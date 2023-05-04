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
    CDS_ID = google_document_ai_processor.cds_broad.id
    LOCATION = var.docai_location
    FIRESTORE_COLLECTION = var.firestore_collection
  }

  # Set the Cloud Function trigger to execute when a PDF file is uploaded to the input bucket
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.input.name
  }
}