output "bucket" {
  value = google_storage_bucket.lake.name
}

output "dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "service_account" {
  value = google_service_account.pipeline.email
}
