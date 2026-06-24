terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# -----------------------------------------------------------------------------
# VPC and Networking (Default VPC for Free Tier simplicity)
# -----------------------------------------------------------------------------
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Security group for EC2 instance (allowing SSH and web traffic)
resource "aws_security_group" "ec2_sg" {
  name        = "fraud_feature_store_ec2_sg"
  description = "Allow inbound SSH and HTTP"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Security group for RDS
resource "aws_security_group" "rds_sg" {
  name        = "fraud_feature_store_rds_sg"
  description = "Allow Postgres traffic from EC2"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_sg.id]
  }
}

# Security group for Redis
resource "aws_security_group" "redis_sg" {
  name        = "fraud_feature_store_redis_sg"
  description = "Allow Redis traffic from EC2"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_sg.id]
  }
}

# -----------------------------------------------------------------------------
# S3 Data Lake (Bronze layer)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "bronze_lake" {
  bucket_prefix = "fraud-feature-store-bronze-"
}

# -----------------------------------------------------------------------------
# RDS PostgreSQL
# -----------------------------------------------------------------------------
resource "random_password" "db_password" {
  length  = 16
  special = false
}
resource "aws_db_instance" "postgres" {
  identifier           = "fraud-feature-store-db"
  allocated_storage    = 20
  engine               = "postgres"
  engine_version       = "16"
  instance_class       = "db.t4g.micro" # Free tier eligible
  username             = "fraud_admin"
  password             = random_password.db_password.result
  parameter_group_name = "default.postgres16"
  skip_final_snapshot  = true
  publicly_accessible  = false
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
}

# -----------------------------------------------------------------------------
# ElastiCache Redis
# -----------------------------------------------------------------------------
resource "aws_elasticache_subnet_group" "redis_subnet_group" {
  name       = "fraud-feature-store-redis-subnet"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "fraud-feature-store-redis"
  engine               = "redis"
  node_type            = "cache.t3.micro" # Free tier eligible
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  engine_version       = "7.0"
  port                 = 6379
  security_group_ids   = [aws_security_group.redis_sg.id]
  subnet_group_name    = aws_elasticache_subnet_group.redis_subnet_group.name
}

# -----------------------------------------------------------------------------
# EC2 Instance
# -----------------------------------------------------------------------------
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_instance" "compute" {
  ami           = data.aws_ami.amazon_linux_2023.id
  instance_type = "t2.micro" # Free tier eligible
  
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]

  user_data = <<-EOF
              #!/bin/bash
              dnf update -y
              dnf install -y docker git
              systemctl start docker
              systemctl enable docker
              usermod -aG docker ec2-user
              
              # Install docker-compose
              curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
              chmod +x /usr/local/bin/docker-compose
              
              # Export database and redis endpoints
              echo "export RDS_ENDPOINT=${aws_db_instance.postgres.endpoint}" >> /home/ec2-user/.bashrc
              echo "export REDIS_ENDPOINT=${aws_elasticache_cluster.redis.cache_nodes[0].address}" >> /home/ec2-user/.bashrc
              EOF

  tags = {
    Name = "fraud_feature_store_compute"
  }
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------
output "s3_bucket_name" {
  value = aws_s3_bucket.bronze_lake.bucket
}

output "rds_endpoint" {
  value = aws_db_instance.postgres.endpoint
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "ec2_public_ip" {
  value = aws_instance.compute.public_ip
}
