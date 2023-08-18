provider_installation {
  network_mirror {
    url = "https://terraform-mirror.yandexcloud.net/"
    include = ["registry.terraform.io/*/*"]
  }

  direct {
    exclude = ["registry.terraform.io/*/*"]
  }
}

plugin_cache_dir = "$HOME/.terraform.d/plugin-cache"
