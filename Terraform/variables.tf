variable "datastore_protocol" {
  type = string
  default = "postgresql"
}

variable "datastore_url" {
  type = string
  default = "mycluster.cluster-123456789012.us-east-1.rds.amazonaws.com"
}

variable "datastore_db_name" {
  type = string
  default = "mydb"
}

variable "datatore_connection_port" {
  type = string
  default = "5432"
}

variable "db_connection_username_key" {
  type = string
  default = "db_connection_username"
}

variable "db_connection_password_key" {
  type = string
  default = "db_connection_password"
}

variable "python_lib_zip" {
  type = string
  default = "utils.zip"
}

variable "support_email" {
  type = string
  default = "data-support@mybigbank.co.za"
}