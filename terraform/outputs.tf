output "lakehouse_bucket" {
  description = "Bucket du lakehouse : valeur de LAKE_BUCKET sur GCP (gs://…)"
  value       = google_storage_bucket.lakehouse.url
}

output "gold_dataset" {
  description = "Dataset BigQuery des tables gold"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "biglake_connection" {
  description = "Connexion BigLake pour lire les tables Iceberg depuis BigQuery"
  value       = google_bigquery_connection.lakehouse.name
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}

output "dashboard_service_account" {
  value = google_service_account.dashboard.email
}
