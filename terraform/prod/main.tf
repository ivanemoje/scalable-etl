# S3 Module
module "datalake" {
  source       = "../modules/s3"
  env          = var.env
  project_name = "my-project"
  bucket_names = ["gold", "silver", "bronze"]
}

# IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "glue-job-role-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Precise IAM Policy using module outputs instead of wildcards
resource "aws_iam_policy" "s3_access" {
  name = "glue-s3-access-${var.env}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = flatten([
        for arn in values(module.datalake.bucket_arns) : [arn, "${arn}/*"]
      ])
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}

# Glue Job using Module Outputs
resource "aws_glue_job" "etl_job" {
  name     = "bronze-to-silver-${var.env}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${module.datalake.bucket_ids["bronze"]}/scripts/etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--BRONZE_PATH"  = "s3://${module.datalake.bucket_ids["bronze"]}/data/"
    "--SILVER_PATH"  = "s3://${module.datalake.bucket_ids["silver"]}/data/"
  }
}