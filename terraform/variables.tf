variable "project_id" {
  description = "The ID of the project"
  type        = string
}

variable "region" {
  description = "The default compute region"
  type        = string
  default     = "europe-west3"
}

variable "zone" {
  description = "The default compute zone"
  type        = string
  default     = "europe-west3-a"
}

variable "suffix" {
  type    = string
  default = "7e2edc13"
}

variable "firestore_collection" {
  type    = string
  default = "docai_showcase"
}

variable "bucket_location" {
  type    = string
  default = "eu"
}

variable "docai_location" {
  type    = string
  default = "eu"
}