variable "region" {
  description = "region of deployment"
  type        = string
  default     = "us-central1"
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "name_prefix" {
  description = "Prefix for naming resources."
  type        = string
  default     = "allica"
}

variable "environment" {
  description = "Composer environment name"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prep", "prod"], var.environment)
    error_message = "environment must be one of 'dev', 'prep', or 'prod'."
  }

}

variable "bucket_name" {
  description = "GCS bucket name for Composer environment storage"
  type        = string
  default     = ""

}

variable "use_private_environment" {
  description = "Whether to enable private environment"
  type        = bool
  default     = false
}

variable "enable_private_builds_only" {
  description = "Whether to enable private builds only"
  type        = bool
  default     = false
}

variable "environment_size" {
  type        = string
  description = "The environment size controls the performance parameters of the managed Cloud Composer infrastructure that includes the Airflow database. Values for environment size are: `ENVIRONMENT_SIZE_SMALL`, `ENVIRONMENT_SIZE_MEDIUM`, and `ENVIRONMENT_SIZE_LARGE`."
  default     = ""
  validation {
    condition     = contains(["", "ENVIRONMENT_SIZE_SMALL", "ENVIRONMENT_SIZE_MEDIUM", "ENVIRONMENT_SIZE_LARGE"], var.environment_size)
    error_message = "environment_size must be one of an empty string, 'ENVIRONMENT_SIZE_SMALL', 'ENVIRONMENT_SIZE_MEDIUM', or 'ENVIRONMENT_SIZE_LARGE'."
  }
}

variable "resilience_mode" {
  description = "The resilience mode of the environment. Possible values are: `STANDARD_RESILIENCE` and `HIGH_RESILIENCE`."
  type        = string
  default     = "STANDARD_RESILIENCE"
  validation {
    condition     = contains(["STANDARD_RESILIENCE", "HIGH_RESILIENCE"], var.resilience_mode)
    error_message = "resilience_mode must be either 'STANDARD_RESILIENCE' or 'HIGH_RESILIENCE'."
  }
}

variable "create_network_attachment" {
  type        = bool
  description = "Either create a new network attachment or use existing one. If true, provide the subnet details."
  default     = false
}

variable "composer_network_attachment_name" {
  type        = string
  description = "Name for PSC (Private Service Connect) Network entry point."
  default     = null
}

variable "network_id" {
  type        = string
  description = "The VPC network to host the composer cluster."
  default     = ""
}

variable "subnetwork_id" {
  type        = string
  description = "The name of the subnetwork to host the composer cluster."
  default     = ""
}

variable "scheduler" {
  type = object({
    cpu        = string
    memory_gb  = number
    storage_gb = number
    count      = number
  })
  description = "Scheduler workload configuration."
  default = {
    cpu        = 0.5
    memory_gb  = 2
    storage_gb = 1
    count      = 1
  }
}

variable "triggerer" {
  type = object({
    cpu       = string
    memory_gb = number
    count     = number
  })
  description = "Triggerer workload configuration."
  default = {
    cpu       = 0.5
    memory_gb = 1
    count     = 1
  }
}

variable "dag_processor" {
  type = object({
    cpu        = string
    memory_gb  = number
    storage_gb = number
    count      = number
  })
  description = "DAG Processor workload configuration."
  default = {
    cpu        = 1
    memory_gb  = 4
    storage_gb = 1
    count      = 2
  }
}

variable "web_server" {
  type = object({
    cpu        = string
    memory_gb  = number
    storage_gb = number
  })
  description = "Web Server workload configuration."
  default = {
    cpu        = 1
    memory_gb  = 2
    storage_gb = 1
  }
}

variable "worker" {
  type = object({
    cpu        = string
    memory_gb  = number
    storage_gb = number
    min_count  = number
    max_count  = number
  })
  description = "Worker workload configuration."
  default = {
    cpu        = 0.5
    memory_gb  = 2
    storage_gb = 10
    min_count  = 2
    max_count  = 3
  }
}

variable "grant_sa_agent_permission" {
  type        = bool
  default     = true
  description = "Cloud Composer relies on Workload Identity as Google API authentication mechanism for Airflow. "
}
