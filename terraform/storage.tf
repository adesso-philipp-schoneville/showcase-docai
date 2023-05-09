
resource "random_id" "random_suffix" {
  byte_length = 4
  keepers = {
    project_id   = var.project_id
    project_zone = var.zone
  }
}

################
# Cloud Storage #
################

# Cloud Storage bucket for ZIP archive files of Cloud Functions
resource "google_storage_bucket" "function_archives" {
  name                        = "showcase_docai_cloud_functions-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = true
}

# Cloud Storage bucket for input PDF data
resource "google_storage_bucket" "input" {
  name                        = "showcase_docai_input-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = true
}

# Cloud Storage bucket for output JSON data
resource "google_storage_bucket" "output" {
  name                        = "showcase_docai_output-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = true
}


# Document AI training and testing data
resource "google_storage_bucket" "docai_data" {
  name                        = "showcase_docai_docai_data-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = false
}

####################
# Processor Buckets #
####################

# CDS Broad
resource "google_storage_bucket" "cds_broad" {
  name                        = "showcase_docai_cds_broad-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = false
}

# CDC Zaehlerstand
resource "google_storage_bucket" "cde_formular_anschreiben" {
  name                        = "showcase_docai_cde_formular_anschreiben-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = false
}

# CDE Formular Widerruf
resource "google_storage_bucket" "cde_formular_formular" {
  name                        = "showcase_docai_cde_formular_formular-${var.suffix}"
  location                    = var.bucket_location
  force_destroy               = false
}


############
# FIRESTORE #
############

# Firestore to store metadata and extracted data
resource "google_firestore_database" "default_database" {
  project = var.project_id

  name = "(default)"

  location_id = "eur3"
  type        = "FIRESTORE_NATIVE"
}