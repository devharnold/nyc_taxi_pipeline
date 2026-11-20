variable "aws_region" {
  description = "Aws region for the project's services"
  type        = string
  default     = "us-east-1"

}

# Unique identifier for the s3 bucket
variable "bucket_name" {
  description = "Name of the s3 bucket"
  type        = string
  default     = "devharnold-nyc-taxi-data-bucket"
}

variable "dataset_name" {
  description = "Glue catalog dataset name(logical dataset for Redshift)"
  type        = string
  default     = "nyc_data"
}
