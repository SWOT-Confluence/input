# Job Definition
resource "aws_batch_job_definition" "generate_batch_jd_input" {
  name = "${var.prefix}-input"
  type = "container"
  platform_capabilities = ["FARGATE"]
  propagate_tags = true
  tags = { "job_definition" : "${var.prefix}-input" }

  container_properties = jsonencode({
    image = "${local.account_id}.dkr.ecr.us-west-2.amazonaws.com/${var.prefix}-input:${var.image_tag}"
    executionRoleArn = var.iam_execution_role_arn
    jobRoleArn = var.iam_job_role_arn
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.cw_log_group.name
      }
    }
    resourceRequirements = [{
      type = "MEMORY"
      value = "1024"
    }, {
      type = "VCPU",
      value = "0.5"
    }]
    mountPoints = [{
      sourceVolume = "input",
      containerPath = "/mnt/data"
      readOnly = false
    }]
    volumes = [{
      name = "input"
      efsVolumeConfiguration = {
        fileSystemId = var.efs_file_system_ids["input"]
        rootDirectory = "/"
      }
    }]
  })
}

# API key parameter
resource "aws_ssm_parameter" "hydrocron_key_parameter" {
  name = "${var.prefix}-hydrocron-key"
  description = "Hydrocron confluence API key"
  type = "SecureString"
  value = var.api_key
}

# Log group
resource "aws_cloudwatch_log_group" "cw_log_group" {
  name = "/aws/batch/job/${var.prefix}-input/"
}
