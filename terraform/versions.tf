terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Remote state via Terraform Cloud's free tier, not a GCS bucket: creating
  # any Cloud Storage bucket requires an active GCP billing account (unlike
  # BigQuery, which has a billing-free sandbox mode), and this project
  # deliberately runs without one. Terraform Cloud gives the same result -
  # state persisted somewhere CI can reach, instead of only on a local disk -
  # without that requirement.
  cloud {
    organization = "devpatel15"
    workspaces {
      name = "batch-elt-pipeline"
    }
  }
}

provider "google" {
  project     = var.project_id
  credentials = file(var.credentials_file)
}
