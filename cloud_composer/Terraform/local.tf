locals {
  region = try("us-central1", var.region)
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
  composer_network_attachment_name = var.composer_network_attachment_name == null ? null : "projects/${var.project_id}/regions/${var.region}/networkAttachments/${var.composer_network_attachment_name}"

  network_project_id = contains(["dev", "prep"], var.environment) ? "tt-labs-002" : "tech-labs-gui"

  cloud_composer_sa = format("service-%s@cloudcomposer-accounts.iam.gserviceaccount.com", "846508348496")

  service_agent_network_role = var.grant_sa_agent_permission && var.create_network_attachment == true ? true : false

  service_project_roles = [
    "roles/composer.worker",
    "roles/bigquery.user",
    #"roles/composer.ServiceAgentV2Ext",
    "roles/bigquery.dataEditor",
    "roles/composer.worker",
  ]

  network_project_roles = [
    "roles/compute.networkUser",
    "roles/composer.sharedVpcAgent"

  ]

  # the service agent need the role roles/composer.ServiceAgentV2Ext on the service project and roles/composer.sharedVpcAgent on the host project

}
