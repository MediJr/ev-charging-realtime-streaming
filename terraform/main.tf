# Infrastructure GCP cible de la plateforme (décision D6 : décrite et validée, pas encore déployée).
#
#   Plateforme locale (Docker)          →  Google Cloud
#   SeaweedFS (S3)                      →  Cloud Storage (même format : Iceberg / Parquet)
#   Catalogue Iceberg JDBC (PostgreSQL) →  BigLake metastore, tables lisibles par BigQuery
#   Spark local                         →  Dataproc Serverless (même code PySpark)
#   PostgreSQL de diffusion + Grafana   →  BigQuery + Grafana (ou Looker Studio)
#
# Le traitement lui-même (Dataproc, Kafka managé) n'est pas décrit ici : ce sont les postes les plus coûteux,
# à dimensionner avec la DSI au moment du passage au cloud.

locals {
  suffix = "${var.project_id}-${var.environment}"
  labels = {
    projet        = "lumio-supervision"
    environnement = var.environment
    gere-par      = "terraform"
  }
}

# --- API nécessaires -----------------------------------------------------------------------------------------------
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "iam.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# --- Lakehouse : un bucket, trois couches ---------------------------------------------------------------------------
resource "google_storage_bucket" "lakehouse" {
  name                        = "lumio-lakehouse-${local.suffix}"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true       # droits gérés uniquement par IAM, pas fichier par fichier
  public_access_prevention    = "enforced" # aucune donnée ne peut devenir publique par erreur
  force_destroy               = var.environment == "dev"
  labels                      = local.labels

  versioning {
    enabled = true # une suppression accidentelle reste récupérable
  }

  # Mesures brutes : supprimées après 90 jours (cadrage, question 7)
  lifecycle_rule {
    condition {
      age            = var.raw_retention_days
      matches_prefix = ["warehouse/bronze/"]
    }
    action {
      type = "Delete"
    }
  }

  # Anciennes versions d'objets : supprimées après 7 jours (sinon le versionnage fait gonfler la facture)
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# --- BigQuery : les tables gold, lues par la supervision et les analystes ---------------------------------------------
resource "google_bigquery_dataset" "gold" {
  dataset_id    = "lumio_gold_${var.environment}"
  friendly_name = "Lumio · supervision des bornes (gold)"
  description   = "Agrégats à la minute, alertes, CO₂ des recharges"
  location      = var.region
  labels        = local.labels

  depends_on = [google_project_service.apis]
}

# Connexion BigLake : BigQuery lit les tables Iceberg du bucket sans les copier
resource "google_bigquery_connection" "lakehouse" {
  connection_id = "lumio-lakehouse-${var.environment}"
  location      = var.region
  friendly_name = "Lakehouse Iceberg Lumio"
  cloud_resource {}

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket_iam_member" "biglake_reads_lake" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.lakehouse.cloud_resource[0].service_account_id}"
}

# --- Comptes de service : un par usage, avec le minimum de droits -----------------------------------------------------
resource "google_service_account" "pipeline" {
  account_id   = "lumio-pipeline-${var.environment}"
  display_name = "Traitement Spark (écrit dans le lakehouse)"
}

resource "google_service_account" "dashboard" {
  account_id   = "lumio-dashboard-${var.environment}"
  display_name = "Supervision (lecture seule des tables gold)"
}

# Le traitement écrit dans le bucket, et seulement dans ce bucket
resource "google_storage_bucket_iam_member" "pipeline_writes_lake" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_writes_gold" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# La supervision lit les tables gold, sans pouvoir rien modifier
resource "google_bigquery_dataset_iam_member" "dashboard_reads_gold" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_project_iam_member" "dashboard_runs_queries" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

# --- Maîtrise des coûts : alerte de budget (si un compte de facturation est fourni) ----------------------------------
data "google_project" "current" {}

resource "google_billing_budget" "monthly" {
  count           = var.billing_account_id == "" ? 0 : 1
  billing_account = var.billing_account_id
  display_name    = "Lumio supervision (${var.environment})"

  budget_filter {
    projects = ["projects/${data.google_project.current.number}"]
  }

  amount {
    specified_amount {
      currency_code = "EUR"
      units         = tostring(var.monthly_budget_eur)
    }
  }

  dynamic "threshold_rules" {
    for_each = [0.5, 0.9, 1.0]
    content {
      threshold_percent = threshold_rules.value
    }
  }
}
