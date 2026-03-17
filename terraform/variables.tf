
variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "eu-west-2"
}

variable "project_name" {
  type = string
  description = "Project name"
  default = "supplychain360"
}

variable "instance_type" {
  type        = string
  description = "ec2 instance type"
  default     = "t3.micro"

}

variable "bucket_name" {
  type        = string
  description = "This is s3 bucket name"
  default     = "supplychain360-data-lake"
  sensitive   = true
}

variable "public_key_path" {
  default = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIF4wJXQ1lwjhauiYH0+rfwodOm7GGRqdIGYe7wrn1hdl PALMPAY@DESKTOP-G7I205U"
}
