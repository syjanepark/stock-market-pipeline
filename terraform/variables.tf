variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "default"
}

variable "bucket_name" {
  description = "Name of the S3 bucket for stock data"
  type        = string
  default     = "jane-stock-market-data-bucket"
}