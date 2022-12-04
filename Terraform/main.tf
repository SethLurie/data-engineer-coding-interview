##################
# IAM            #
##################

data "aws_iam_policy_document" "glue_assume_role_policy_document" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["glue.amazonaws.com"]
      type = "AWS"
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name = "GlueCatalogRole"
  description = "Role assumed by the glue catalogger"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy_document.json
}

resource "aws_iam_role_policy_attachment" "glue_attatch" {
  role = aws_iam_role.glue_role.name
  # See managed policy here: https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1&skipRegion=true#/policies/arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole$serviceLevelSummary
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "kms_s3_role_document" {
  statement {
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:ReEncryptTo",
      "kms:DescribeKey",
      "kms:ReEncryptFrom"
    ]
    principals {
      identifiers = ["glue.amazonaws.com"]
      type = "AWS"
    }
  }
}

resource "aws_iam_policy" "kms_s3_role" {
  name = "KMSS3Policy"
  policy = data.aws_iam_policy_document.kms_s3_role_document.json
}

resource "aws_iam_role_policy_attachment" "glue_kms_attatch" {
  role = aws_iam_role.glue_role.name
  # See managed policy here: https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1&skipRegion=true#/policies/arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole$serviceLevelSummary
  policy_arn = aws_iam_policy.kms_s3_role.arn
}

data "aws_iam_policy_document" "sns_topic_policy" {
  statement {
    effect  = "Allow"
    actions = ["SNS:Publish"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sns_topic.glue_job_failure.arn]
  }
}

resource "aws_sns_topic_policy" "default" {
  arn    = aws_sns_topic.glue_job_failure.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

###################
# Secrets         #
###################

data "aws_secretsmanager_secret" "db_connection_username" {
  name = "db_connection_username"
}

data "aws_secretsmanager_secret_version" "db_connection_username" {
  secret_id = data.aws_secretsmanager_secret.db_connection_username.id
}

data "aws_secretsmanager_secret" "db_connection_password" {
  name = "db_connection_password"
}

data "aws_secretsmanager_secret_version" "db_connection_password" {
  secret_id = data.aws_secretsmanager_secret.db_connection_password.id
}

locals {
  db_connection_username = data.aws_secretsmanager_secret_version.db_connection_username.secret_string
  db_connection_password = data.aws_secretsmanager_secret_version.db_connection_password.secret_string
}

###################
# Glue Connection #
###################
resource "aws_glue_connection" "glue_connection" {
  name = "Datastore Connection"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:${var.datastore_protocol}://${var.datastore_url}:${var.datatore_connection_port}/${var.datastore_db_name}"
    PASSWORD = local.db_connection_password
    USERNAME = local.db_connection_username
  }
}

##################
# Glue Catalog   #
##################

resource "aws_glue_catalog_database" "catalog_db" {
  name = "${var.datastore_db_name}-catalog"
}

##################
# Glue Crawler   #
##################

resource "aws_glue_crawler" "crawler" {
  database_name = aws_glue_catalog_database.catalog_db.name
  name = "catalog_db"
  role = aws_iam_role.glue_role.arn
  jdbc_target {
    connection_name = aws_glue_connection.glue_connection.name
    path = "${var.datastore_db_name}/%"
  }
}

##################
# Glue Job       #
##################

resource "aws_glue_job" "monthly_aggregator" {
  name = "monthly_aggregator"
  role_arn = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.etl_files.bucket}/${aws_s3_object.etl_file.key}"
  }

  default_arguments = {
    "extra-py-files" = "s3://${aws_s3_bucket.etl_files.bucket}/${aws_s3_object.utils_zip.key}"
    "--continuous-log-logGroup" = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter" = "true"
    "--enable-metrics" = "true"
  }
}

##################
# Glue Triggers  #
##################

resource "aws_glue_trigger" "monthly_datastore_crawl" {
  name = "monthly_datastore_crawl"
  # run 1 minute past midnight on the first of every month
  schedule = "cron(1, 0, 1, *, *, ?, *)"
  type = "SCHEDULED"
  actions {
    crawler_name = aws_glue_crawler.crawler.name
  }
}

resource "aws_glue_trigger" "monthly_data_aggregate" {
  name = "monthly_data_aggregate"
  type = "CONDITIONAL"

  actions {
    job_name = aws_glue_job.monthly_aggregator.name
  }
  predicate {
    conditions {
      crawler_name = aws_glue_crawler.crawler.name
      crawl_state = "SUCCEEDED"
    }
  }
}

##################
# S3             #
##################
resource "aws_s3_bucket" "glue_output" {
  bucket = "banking-group-${var.datastore_db_name}-reports"
}

resource "aws_s3_bucket" "etl_files" {
  bucket = "banking-group-${var.datastore_db_name}-etl"
}

resource "aws_s3_object" "etl_file" {
  source = "${path.module}/../Glue/etl.py"
  # Will reupload the file when hash changes
  etag = "${filemd5("${path.module}/../Glue/etl.py")}"
  bucket = aws_s3_bucket.etl_files.bucket
  key = "etl.py"
}

data "archive_file" "utils_files" {
  type = "zip"
  source_dir = "${path.module}/../Glue"
  excludes = ["etl.py", "test/*"]
  output_path = "${path.module}/build/${var.python_lib_zip}"
}

resource "aws_s3_object" "utils_zip" {
  source = "${path.module}/build/${var.python_lib_zip}"
    # Will reupload the file when hash changes
  etag = "${filemd5("${path.module}/build/${var.python_lib_zip}")}"
  bucket = aws_s3_bucket.etl_files.bucket
  key = "${var.python_lib_zip}"
}

##################
# SNS            #
##################

resource "aws_sns_topic" "glue_job_failure" {
  name = "glue_job_failure"
}

resource "aws_sns_topic_subscription" "glue_job_failure_subscription" {
  topic_arn = aws_sns_topic.glue_job_failure.arn
  protocol = "email"
  endpoint = var.support_email
}

resource "aws_cloudwatch_event_rule" "glue_job_failure_rule" {
  name = "glue_job_failure_rule"
  description = "Alerts when a glue job fails"
  event_pattern = <<EOF
  {
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {"state": ["FAILED"]}
  }
  EOF
}

resource "aws_cloudwatch_event_target" "glue_job_failure_target" {
  rule = aws_cloudwatch_event_rule.glue_job_failure_rule.name
  arn = aws_sns_topic.glue_job_failure.arn
  input_transformer {
    input_paths = {
      jobName = "$.detail.jobName"
      event = "$"
    }
    input_template = <<EOF
      {
        "subject" = <jobName>,
        "event" = <event>
      }
    EOF
  }
}

##################
# CloudWatch     #
##################
resource "aws_cloudwatch_log_group" "glue_logs" {
  name = "glue_logs"
  retention_in_days = 365
}

##################
# KMS            #
##################
resource "aws_kms_key" "etl_key" {
  description  = "This key is used to encrypt bucket objects"
  deletion_window_in_days = 10
}

resource "aws_kms_key" "glue_output_key" {
  description  = "This key is used to encrypt bucket objects"
  deletion_window_in_days = 10
}

resource "aws_s3_bucket_server_side_encryption_configuration" "etl_sse" {
  bucket = aws_s3_bucket.etl_files.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.etl_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_output_sse" {
  bucket = aws_s3_bucket.glue_output.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.glue_output_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}
