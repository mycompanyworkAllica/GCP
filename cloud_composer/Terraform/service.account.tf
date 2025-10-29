resource "google_service_account" "composer" {
  project      = var.project_id
  account_id   = "composer-sa-${var.environment}"
  description  = "Test Service Account for Composer Environment"
  display_name = "Composer Service Account"
}

resource "google_project_iam_member" "service-project" {
  for_each   = toset(local.service_project_roles)
  project    = var.project_id
  role       = each.value
  member     = "serviceAccount:${google_service_account.composer.email}"
  depends_on = [google_service_account.composer]
}

resource "google_project_iam_member" "network-project" {
  for_each   = toset(local.network_project_roles)
  project    = local.network_project_id
  role       = each.value
  member     = "serviceAccount:${google_service_account.composer.email}"
  depends_on = [google_service_account.composer]
}

#### SERVICE AGENT ROLES  #####     

resource "google_project_iam_member" "composer_agent_service_account" {
  count   = var.grant_sa_agent_permission ? 1 : 0
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = format("serviceAccount:%s", local.cloud_composer_sa)
}

#resource "google_project_iam_member" "composer_agent_host_account" {
#  count   = local.service_agent_network_role ? 1 : 0
#  project = local.network_project_id
#  role    = "roles/composer.sharedVpcAgent"
#  member  = format("serviceAccount:%s", local.cloud_composer_sa)
#}

#resource "google_service_account_iam_member" "composer_act_as" {
#  service_account_id = "projects/tech-labs-gui/serviceAccounts/${local.cloud_composer_sa}"
#  role               = "roles/iam.serviceAccountUser"
#  member             = "serviceAccount:terraform@organization-base-project.iam.gserviceaccount.com"
#}
