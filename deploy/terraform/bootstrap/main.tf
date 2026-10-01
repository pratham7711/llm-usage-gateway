# One-time setup in a personal AWS account. Everything here is cheap to keep: three ECR repos
# that hold at most 3 images each, a reports bucket whose objects expire, an OIDC provider, a CI
# role and a budget alarm. The billable environment lives in ../stack and is destroyed after
# every run.
#
#   cd deploy/terraform/bootstrap
#   terraform init && terraform apply -var alert_email=you@example.com
#
# Then set the outputs as GitHub repository variables (AWS_REGION, AWS_CI_ROLE_ARN,
# REPORTS_BUCKET) and run the "aws-bench" workflow.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.80" }
  }
}

provider "aws" {
  region = var.region
  default_tags { tags = { Project = "llmgw" } }
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "github_repo" {
  description = "owner/name of the repository whose main branch may deploy"
  type        = string
  default     = "pratham7711/llm-usage-gateway"
}

variable "alert_email" {
  description = "Where budget alerts go"
  type        = string
}

variable "monthly_budget_usd" {
  type    = number
  default = 10
}

variable "report_retention_days" {
  type    = number
  default = 30
}

data "aws_caller_identity" "me" {}

locals {
  services = ["gateway", "metering", "mock-upstream"]
  account  = data.aws_caller_identity.me.account_id
}

# --- Images -----------------------------------------------------------------------------------

resource "aws_ecr_repository" "svc" {
  for_each             = toset(local.services)
  name                 = "llmgw/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_ecr_lifecycle_policy" "svc" {
  for_each   = aws_ecr_repository.svc
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 3 newest images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 3 }
      action       = { type = "expire" }
    }]
  })
}

# --- Reports and stack state ------------------------------------------------------------------

resource "aws_s3_bucket" "reports" {
  bucket_prefix = "llmgw-reports-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "reports" {
  bucket                  = aws_s3_bucket.reports.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id
  rule {
    id     = "expire-runs"
    status = "Enabled"
    filter { prefix = "runs/" }
    expiration { days = var.report_retention_days }
  }
  rule {
    id     = "abort-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}

# --- GitHub Actions OIDC ----------------------------------------------------------------------

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "ci_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "ci" {
  name                 = "llmgw-ci"
  assume_role_policy   = data.aws_iam_policy_document.ci_trust.json
  max_session_duration = 7200
}

data "aws_iam_policy_document" "ci" {
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid = "EcrPush"
    actions = [
      "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload", "ecr:PutImage", "ecr:UploadLayerPart", "ecr:DescribeImages",
    ]
    resources = [for r in aws_ecr_repository.svc : r.arn]
  }
  statement {
    sid       = "Reports"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.reports.arn}/*"]
  }
  statement {
    sid       = "ReportsList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.reports.arn]
  }
  statement {
    sid = "StackEc2"
    actions = [
      "ec2:Describe*", "ec2:CreateVpc", "ec2:DeleteVpc", "ec2:ModifyVpcAttribute",
      "ec2:CreateSubnet", "ec2:DeleteSubnet", "ec2:ModifySubnetAttribute",
      "ec2:CreateInternetGateway", "ec2:DeleteInternetGateway", "ec2:AttachInternetGateway", "ec2:DetachInternetGateway",
      "ec2:CreateRouteTable", "ec2:DeleteRouteTable", "ec2:CreateRoute", "ec2:DeleteRoute",
      "ec2:AssociateRouteTable", "ec2:DisassociateRouteTable",
      "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress", "ec2:RevokeSecurityGroupEgress",
      "ec2:RunInstances", "ec2:TerminateInstances", "ec2:CreateTags", "ec2:DeleteTags",
      "ec2:ModifyInstanceAttribute",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.region]
    }
  }
  statement {
    sid = "StackNodeRole"
    actions = [
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:TagRole", "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies", "iam:AttachRolePolicy", "iam:DetachRolePolicy",
      "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy", "iam:ListInstanceProfilesForRole",
      "iam:CreateInstanceProfile", "iam:DeleteInstanceProfile", "iam:GetInstanceProfile",
      "iam:AddRoleToInstanceProfile", "iam:RemoveRoleFromInstanceProfile", "iam:TagInstanceProfile",
    ]
    resources = [
      "arn:aws:iam::${local.account}:role/llmgw-node",
      "arn:aws:iam::${local.account}:instance-profile/llmgw-node",
    ]
  }
  statement {
    sid       = "PassNodeRole"
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::${local.account}:role/llmgw-node"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com"]
    }
  }
  statement {
    sid       = "AmiLookup"
    actions   = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = ["arn:aws:ssm:${var.region}::parameter/aws/service/ami-amazon-linux-latest/*"]
  }
  statement {
    sid       = "RunOnNode"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:aws:ec2:${var.region}:${local.account}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = ["llmgw"]
    }
  }
  statement {
    sid       = "RunShellDocument"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:aws:ssm:${var.region}::document/AWS-RunShellScript"]
  }
  statement {
    sid       = "CommandStatus"
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ci" {
  name   = "llmgw-ci"
  role   = aws_iam_role.ci.id
  policy = data.aws_iam_policy_document.ci.json
}

# --- Cost alarm -------------------------------------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  name         = "llmgw-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

output "region" { value = var.region }
output "ci_role_arn" { value = aws_iam_role.ci.arn }
output "reports_bucket" { value = aws_s3_bucket.reports.bucket }
output "ecr_registry" { value = "${local.account}.dkr.ecr.${var.region}.amazonaws.com" }
