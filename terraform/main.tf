resource "google_storage_bucket" "lake" {
  name                        = "${var.project_id}-prj1-lake-${var.env}"
  location                    = var.region # a single free-tier region, not the US multi-region
  uniform_bucket_level_access = true
  force_destroy               = var.env == "dev"

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                  = "prj1_gold_${var.env}"
  location                    = var.bq_location
  default_table_expiration_ms = null
}

resource "google_bigquery_table" "fact_vehicle_sales" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "fact_vehicle_sales"
  deletion_protection = false

  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "sale_date"
  }

  clustering = ["plant_id", "dealer_id"]

  schema = jsonencode([
    { name = "sale_id", type = "STRING", mode = "REQUIRED" },
    { name = "sale_date", type = "DATE", mode = "REQUIRED" },
    { name = "plant_id", type = "STRING", mode = "REQUIRED" },
    { name = "dealer_id", type = "STRING", mode = "REQUIRED" },
    { name = "sale_amount", type = "NUMERIC", mode = "REQUIRED" }
  ])
}

resource "google_service_account" "pipeline" {
  account_id   = "prj1-pipeline-${var.env}"
  display_name = "PRJ-1 pipeline"
}

resource "google_bigquery_dataset_iam_member" "writer" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "lake_rw" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}
