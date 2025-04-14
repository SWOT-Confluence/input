# Job Definition
resource "aws_batch_job_definition" "generate_batch_jd_input" {
  name                  = "${var.prefix}-input"
  type                  = "container"
  container_properties  = <<CONTAINER_PROPERTIES
  {
    "image": "${local.account_id}.dkr.ecr.us-west-2.amazonaws.com/${var.prefix}-input",
    "executionRoleArn": "${data.aws_iam_role.exe_role.arn}",
    "jobRoleArn": "${data.aws_iam_role.job_role.arn}",
    "fargatePlatformConfiguration": { "platformVersion": "LATEST" },
    "logConfiguration": {
      "logDriver" : "awslogs",
      "options": {
        "awslogs-group" : "${data.aws_cloudwatch_log_group.cw_log_group.name}"
      }
    },
    "resourceRequirements": [
      {"type": "MEMORY", "value": "1024"},
      {"type": "VCPU", "value": "0.5"}
    ],
    "mountPoints": [
      {
        "sourceVolume": "input",
        "containerPath": "/mnt/data"
      }
    ],
    "volumes": [
      {
        "name": "input",
        "efsVolumeConfiguration": {
          "fileSystemId": "${data.aws_efs_file_system.aws_efs_input.file_system_id}",
          "rootDirectory": "/"
        }
      }
    ]
  }
  CONTAINER_PROPERTIES
  platform_capabilities = ["FARGATE"]
  propagate_tags        = true
  tags                  = { "job_definition" : "${var.prefix}-input" }
}

# API key parameter
resource "aws_ssm_parameter" "hydrocron_key_parameter" {
  name        = "${var.prefix}-hydrocron-key"
  description = "Hydrocron confluence API key"
  type        = "SecureString"
  value       = var.api_key
  overwrite   = true
}