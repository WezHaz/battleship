module "vpc" {
  source      = "../../modules/vpc"
  project_name = var.project_name
  environment = var.environment
  tags        = var.tags
}

module "ecs" {
  source       = "../../modules/ecs"
  cluster_name = "${var.project_name}-${var.environment}-cluster"
  tags         = var.tags
}

module "iam" {
  source       = "../../modules/iam"
  project_name = var.project_name
  environment  = var.environment
  tags         = var.tags
}

module "rds" {
  source             = "../../modules/rds"
  project_name       = var.project_name
  environment        = var.environment
  vpc_id             = module.vpc.vpc_id
  vpc_cidr_block     = module.vpc.vpc_cidr_block
  private_subnet_ids = module.vpc.private_subnet_ids
  db_username        = var.db_username
  db_password        = var.db_password
  instance_class     = "db.t4g.micro"
  allocated_storage  = 20
  multi_az           = false
  tags               = var.tags
}

module "assets_bucket" {
  source        = "../../modules/s3"
  bucket_name   = "${var.project_name}-${var.environment}-assets"
  force_destroy = true
  tags          = var.tags
}
