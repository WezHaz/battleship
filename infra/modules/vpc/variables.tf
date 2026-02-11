variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "cidr_block" {
  type    = string
  default = "10.42.0.0/16"
}

variable "public_subnet_count" {
  type    = number
  default = 2
}

variable "private_subnet_count" {
  type    = number
  default = 2
}

variable "tags" {
  type    = map(string)
  default = {}
}
