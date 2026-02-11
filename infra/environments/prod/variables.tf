variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "operation-battleship"
}

variable "environment" {
  type    = string
  default = "prod"
}

variable "db_username" {
  type    = string
  default = "battleship"
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "tags" {
  type = map(string)
  default = {
    Owner   = "platform"
    Managed = "terraform"
    Tier    = "production"
  }
}
