terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  profile = var.profile
}

# S3 bucket for storing stock data
resource "aws_s3_bucket" "stock_data" {
  bucket = var.bucket_name

  tags = {
    Name        = "stock-market-pipeline"
    Environment = "dev"
    Project     = "Stock Market Pipeline"
  }
}

# S3 bucket versioning (optional but good practice)
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.stock_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# IAM role for Airflow access
resource "aws_iam_role" "airflow_role" {
  name = "airflow-s3-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

# IAM policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "airflow-s3-access-policy"
  description = "Policy granting Airflow access to S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.stock_data.arn,
          "${aws_s3_bucket.stock_data.arn}/*"
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "attach_policy" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}