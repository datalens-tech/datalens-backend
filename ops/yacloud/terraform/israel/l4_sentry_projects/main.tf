locals {
  env_name = "israel"
}

module "constants" {
  source   = "../../_modules/_constants/v1"
  env_name = local.env_name
}

module "sentry_projects" {
  source = "../../_modules/_main_sentry_projects/v1"

  projects = {
    dataset_api = {
      platform = "python-flask"
    }
    dataset_data_api = {
      platform = "python-aiohttp"
    }
    file_uploader_api = {
      platform = "python-aiohttp"
    }
    file_uploader_worker = {
      platform = "python"
    }
    external_api = {
      platform = "python-aiohttp"
    }
  }

  users = module.constants.users

}
