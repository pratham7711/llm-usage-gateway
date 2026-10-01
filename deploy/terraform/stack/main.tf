# The billable environment for one benchmark run: a VPC with one public subnet (no NAT gateway),
# one Graviton instance running k3s, and the instance's IAM role. CI applies it, runs the bench
# and destroys it in the same job. As a second guard the instance schedules its own shutdown,
# which terminates it, after max_lifetime_minutes even if nobody runs destroy.
#
# State lives in the bootstrap reports bucket:
#   terraform init -backend-config="bucket=<reports_bucket>" -backend-config="region=<region>"

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.80" }
  }
  backend "s3" {
    key          = "tfstate/stack.tfstate"
    use_lockfile = true
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

variable "reports_bucket" {
  type = string
}

variable "instance_type" {
  description = "Graviton (arm64); the images are built for linux/arm64"
  type        = string
  default     = "t4g.xlarge"
}

variable "k3s_version" {
  type    = string
  default = "v1.35.5+k3s1"
}

variable "max_lifetime_minutes" {
  type    = number
  default = 90
}

variable "ingress_cidrs" {
  description = "CIDRs allowed to reach the gateway on port 80. Empty by default: the bench runs inside the cluster."
  type        = list(string)
  default     = []
}

data "aws_caller_identity" "me" {}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

data "aws_availability_zones" "up" {
  state = "available"
}

locals {
  registry = "${data.aws_caller_identity.me.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

# --- Network ----------------------------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "llmgw" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = data.aws_availability_zones.up.names[0]
  map_public_ip_on_launch = true
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "node" {
  name        = "llmgw-node"
  description = "k3s node: no SSH (SSM only), optional HTTP to the gateway ingress"
  vpc_id      = aws_vpc.this.id

  dynamic "ingress" {
    for_each = length(var.ingress_cidrs) > 0 ? [1] : []
    content {
      description = "gateway ingress"
      from_port   = 80
      to_port     = 80
      protocol    = "tcp"
      cidr_blocks = var.ingress_cidrs
    }
  }

  egress {
    description = "package, image and k3s downloads; SSM"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Node identity ----------------------------------------------------------------------------

data "aws_iam_policy_document" "node_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "llmgw-node"
  assume_role_policy = data.aws_iam_policy_document.node_trust.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ecr_read" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

data "aws_iam_policy_document" "node_reports" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:aws:s3:::${var.reports_bucket}/runs/*"]
  }
}

resource "aws_iam_role_policy" "node_reports" {
  name   = "reports"
  role   = aws_iam_role.node.id
  policy = data.aws_iam_policy_document.node_reports.json
}

resource "aws_iam_instance_profile" "node" {
  name = "llmgw-node"
  role = aws_iam_role.node.name
}

# --- The node ---------------------------------------------------------------------------------

resource "aws_instance" "node" {
  ami                                  = data.aws_ssm_parameter.al2023_arm64.value
  instance_type                        = var.instance_type
  subnet_id                            = aws_subnet.public.id
  vpc_security_group_ids               = [aws_security_group.node.id]
  iam_instance_profile                 = aws_iam_instance_profile.node.name
  instance_initiated_shutdown_behavior = "terminate"

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
    encrypted             = true
  }

  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    k3s_version          = var.k3s_version
    registry             = local.registry
    region               = var.region
    max_lifetime_minutes = var.max_lifetime_minutes
  })

  tags = { Name = "llmgw-k3s" }
}

output "instance_id" { value = aws_instance.node.id }
output "public_ip" { value = aws_instance.node.public_ip }
output "registry" { value = local.registry }
