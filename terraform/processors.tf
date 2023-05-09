# CDC Broad Document AI Processor
resource "google_document_ai_processor" "cds_broad" {
  location     = var.docai_location
  type         = "CUSTOM_SPLITTING_PROCESSOR"
  display_name = "CDS Broad"

  lifecycle {
    prevent_destroy = true
  }
}

# CDE Formular Widerruf Document AI Processor
resource "google_document_ai_processor" "cde_formular_anschreiben" {
  location     = var.docai_location
  type         = "CUSTOM_EXTRACTION_PROCESSOR"
  display_name = "CDE KFZ Anschreiben"

  lifecycle {
    prevent_destroy = true
  }
}

# CDE Formular Formular Document AI Processor
resource "google_document_ai_processor" "cde_formular_formular" {
  location     = var.docai_location
  type         = "CUSTOM_EXTRACTION_PROCESSOR"
  display_name = "CDE KFZ Formular"

  lifecycle {
    prevent_destroy = true
  }
}