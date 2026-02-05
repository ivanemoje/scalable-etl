resource "aws_iam_role" "glue_role" {
  name = "${var.job_name}-role-${var.env}"

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

resource "aws_iam_policy" "s3_access" {
  name = "${var.job_name}-s3-policy-${var.env}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = concat(var.s3_arns, [for arn in var.s3_arns : "${arn}/*"])
    }]
  })
}

resource "aws_iam_role_policy_attachment" "s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}

resource "aws_glue_job" "this" {
  name     = "${var.job_name}-${var.env}"
  role_arn = aws_iam_role.glue_role.arn
  command {
    script_location = var.script_location
    python_version  = "3"
  }
}