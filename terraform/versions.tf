terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.8"
    }
  }

  # En équipe, l'état serait partagé dans un bucket GCS (verrouillage, historique) :
  # backend "gcs" { bucket = "lumio-tfstate", prefix = "streaming" }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
