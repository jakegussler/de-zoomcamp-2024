variable "project" {
  description = "GCP Project"
  default     = "advance-archery-411422"
}


variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Project region"
  default     = "us-central1"
}


variable "credentials" {
  description = "JSON credentials for Google Cloud"
  type        = string
  default = "./keys/my-creds.json"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  default     = "advance-archery-411422-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
