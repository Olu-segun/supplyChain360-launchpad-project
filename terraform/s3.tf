resource "aws_s3_bucket" "supplychain360-data-lake" {
  bucket = var.bucket_name

  force_destroy = true

  tags = {
    Name        = "supplychain360-bucket"
  }
}
