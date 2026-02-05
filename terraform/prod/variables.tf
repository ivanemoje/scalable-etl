variable "aws_region" {
  default = "eu-central-1"
}

variable "env" {
  default = "prod"
}

variable "bucket_names" {
  type    = list(string)
  default = ["bronze", "silver", "gold"]
}