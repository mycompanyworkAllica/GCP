resource "google_composer_environment" "test" {

  project = var.project_id
  name    = "${var.name_prefix}-composer-${var.environment}"
  region  = local.region
  labels  = local.labels

  dynamic "storage_config" {
    for_each = var.bucket_name != "" ? ["storage_config"] : []
    content {
      bucket = var.bucket_name
    }
  }

  config {
    enable_private_environment = var.use_private_environment
    enable_private_builds_only = var.enable_private_builds_only
    environment_size           = (var.environment_size != "" ? var.environment_size : null)
    resilience_mode            = var.resilience_mode

    node_config {
      service_account             = try(google_service_account.composer.email, null)
      network                     = var.create_network_attachment ? var.network_id : null
      subnetwork                  = var.create_network_attachment ? var.subnetwork_id : null
      composer_network_attachment = var.create_network_attachment ? null : local.composer_network_attachment_name
    }

    software_config {
      image_version = "composer-3-airflow-2"
    }

    dynamic "workloads_config" {
      for_each = var.environment_size == "" ? ["workloads_config"] : []
      content {
        dynamic "scheduler" {
          for_each = var.scheduler != null ? [var.scheduler] : []
          content {
            cpu        = scheduler.value["cpu"]
            memory_gb  = scheduler.value["memory_gb"]
            storage_gb = scheduler.value["storage_gb"]
            count      = scheduler.value["count"]
          }
        }
        dynamic "triggerer" {
          for_each = var.triggerer != null ? [var.triggerer] : []
          content {
            cpu       = triggerer.value["cpu"]
            memory_gb = triggerer.value["memory_gb"]
            count     = triggerer.value["count"]
          }
        }
        dynamic "dag_processor" {
          for_each = var.dag_processor != null ? [var.dag_processor] : []
          content {
            cpu        = dag_processor.value["cpu"]
            memory_gb  = dag_processor.value["memory_gb"]
            storage_gb = dag_processor.value["storage_gb"]
            count      = dag_processor.value["count"]
          }
        }
        dynamic "web_server" {
          for_each = var.web_server != null ? [var.web_server] : []
          content {
            cpu        = web_server.value["cpu"]
            memory_gb  = web_server.value["memory_gb"]
            storage_gb = web_server.value["storage_gb"]
          }
        }
        dynamic "worker" {
          for_each = var.worker != null ? [var.worker] : []
          content {
            cpu        = worker.value["cpu"]
            memory_gb  = worker.value["memory_gb"]
            storage_gb = worker.value["storage_gb"]
            min_count  = worker.value["min_count"]
            max_count  = worker.value["max_count"]
          }
        }
      }
    }
  }

  depends_on = [
    google_service_account.composer,
    #google_project_iam_member.composer_agent_host_account,
    google_project_iam_member.composer_agent_service_account,
    #google_service_account_iam_member.composer_act_as
  ]
}

