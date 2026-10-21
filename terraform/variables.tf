variable "project_id" {
  type        = string
  description = "Your GCP project id. Set it in terraform.tfvars."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bq_location" {
  type    = string
  default = "US"
}

variable "env" {
  type    = string
  default = "dev"
}
