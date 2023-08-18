data "yandex_lockbox_secret_version" "passwords" {
  secret_id = var.secret_id
}

locals {
  passwords = { for entry in data.yandex_lockbox_secret_version.passwords.entries : entry.key => entry.text_value }
}

locals {
  kafka_user_vector_password   = local.passwords[var.users.vector]
  kafka_user_transfer_password = local.passwords[var.users.transfer]
}

resource "yandex_mdb_kafka_cluster" "backend_kafka" {
  name        = var.cluster_name
  environment = "PRODUCTION"
  network_id  = var.network_id
  subnet_ids  = var.subnet_ids

  labels = var.disable_autopurge ? { mdb-auto-purge = "off" } : {}

  config {
    version          = var.kafka_settings.version
    brokers_count    = var.kafka_settings.brokers_count
    zones            = var.kafka_settings.zones
    assign_public_ip = var.kafka_settings.public_ip
    unmanaged_topics = false
    schema_registry  = false
    access {
      data_transfer = true
    }
    kafka {
      resources {
        resource_preset_id = var.kafka_settings.resource_preset_id
        disk_type_id       = "network-ssd"
        disk_size          = var.kafka_settings.disk_size
      }
      kafka_config {
        compression_type                = "COMPRESSION_TYPE_ZSTD"
        log_flush_interval_messages     = 1024
        log_flush_interval_ms           = 1000
        log_flush_scheduler_interval_ms = 1000
        log_retention_bytes             = 1073741824
        log_retention_hours             = 168
        log_retention_minutes           = 10080
        log_retention_ms                = 86400000
        log_segment_bytes               = 134217728
        log_preallocate                 = false
        default_replication_factor      = 1
      }
    }
    zookeeper {
      resources {
        resource_preset_id = var.kafka_settings.zookeeper_resource_preset_id
        disk_type_id       = "network-ssd"
        disk_size          = 10
      }
    }
  }

  user {
    name     = var.users.vector
    password = local.kafka_user_vector_password
    dynamic "permission" {
      for_each = var.topics
      content {
        topic_name = permission.value
        role       = "ACCESS_ROLE_PRODUCER"
      }
    }
    dynamic "permission" {
      for_each = var.topics
      content {
        topic_name = permission.value
        role       = "ACCESS_ROLE_CONSUMER"
      }
    }
  }

  user {
    name     = var.users.transfer
    password = local.kafka_user_transfer_password
    permission {
      topic_name = var.topics.filtered_logs_topic
      role       = "ACCESS_ROLE_CONSUMER"
    }
    permission {
      topic_name = var.topics.unparsed_logs_topic
      role       = "ACCESS_ROLE_CONSUMER"
    }
    permission {
      topic_name = var.topics.usage_tracking_topic
      role       = "ACCESS_ROLE_CONSUMER"
    }
  }
}

resource "yandex_mdb_kafka_topic" "kafka_topic" {
  for_each = var.topics

  cluster_id         = yandex_mdb_kafka_cluster.backend_kafka.id
  name               = each.value
  partitions         = 1
  replication_factor = 1
  topic_config {
    cleanup_policy        = "CLEANUP_POLICY_DELETE"
    compression_type      = "COMPRESSION_TYPE_LZ4"
    file_delete_delay_ms  = 60000
    flush_messages        = 128
    flush_ms              = 1000
    min_compaction_lag_ms = 0
    retention_bytes       = 5368709120
    retention_ms          = 172800000
    max_message_bytes     = 1048588
    min_insync_replicas   = 1
    segment_bytes         = 268435456
    preallocate           = false
  }
}
