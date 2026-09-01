variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "credentials_file" {
  description = "Path to the service account JSON key"
  type        = string
  default     = "../secrets/gcp-service-account.json"
}
