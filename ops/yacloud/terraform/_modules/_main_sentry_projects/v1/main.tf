resource "sentry_team" "dlback" {
  organization = "sentry"

  name = "datalens-backend"
  slug = "dlback"
}

resource "sentry_project" "dlback_app" {
  for_each = var.projects

  organization = "sentry"

  teams = [sentry_team.dlback.slug]
  name  = each.key
  slug  = each.key

  platform    = each.value["platform"]
  resolve_age = 720
}

resource "sentry_key" "dlback_app_key" {
  for_each = sentry_project.dlback_app

  organization = "sentry"

  project = each.value.slug
  name    = "for_app"
}

resource "sentry_organization_member" "dlback_obedient" {
  for_each = toset(var.users)

  organization = "sentry"
  email        = each.key
  role         = "admin"
  teams        = [sentry_team.dlback.slug]
}
