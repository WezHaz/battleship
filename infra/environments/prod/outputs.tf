output "vpc_id" {
  value = module.vpc.vpc_id
}

output "ecs_cluster_arn" {
  value = module.ecs.cluster_arn
}

output "rds_endpoint" {
  value = module.rds.endpoint
}

output "assets_bucket" {
  value = module.assets_bucket.bucket_id
}
