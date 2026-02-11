# Infrastructure (Terraform)

This folder contains reusable Terraform modules and per-environment compositions.

## Layout

- `modules/`: reusable building blocks (`vpc`, `ecs`, `rds`, `s3`, `iam`)
- `environments/dev`: lower-cost baseline
- `environments/prod`: production-oriented baseline

## Usage

```bash
# Initialize and plan development infra
terraform -chdir=infra/environments/dev init
terraform -chdir=infra/environments/dev plan

# Initialize and plan production infra
terraform -chdir=infra/environments/prod init
terraform -chdir=infra/environments/prod plan
```

## Included resources

- VPC with public/private subnets
- ECS cluster with CloudWatch logs
- RDS PostgreSQL instance
- S3 bucket with encryption and versioning
- IAM task execution role for ECS

Adjust module variables before applying in shared accounts.
