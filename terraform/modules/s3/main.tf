locals {
  tier_days = {
    "gold"   = 30
    "silver" = 60
    "bronze" = 90
  }
}

resource "aws_s3_bucket" "this" {
  for_each      = toset(var.bucket_names)
  bucket        = "${var.project_name}-${each.key}-${var.env}"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration_v2" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    id     = "tier-based-expiration"
    status = "Enabled"

    expiration {
      # Looks up the key (gold/silver/bronze) in the local map
      days = lookup(local.tier_days, each.key, 90) 
    }
  }
}

output "bucket_ids" {
  value = { for k, v in aws_s3_bucket.this : k => v.id }
}

output "bucket_arns" {
  value = { for k, v in aws_s3_bucket.this : k => v.arn }
}