output "bucket_name" {
  value = aws_s3_bucket.stock_data.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.stock_data.arn
}

output "airflow_role_arn" {
  value = aws_iam_role.airflow_role.arn
}