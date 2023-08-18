resource "kubernetes_namespace" "sidecar_ns" {
  metadata {
    name = local.namespace
  }
}

resource "kubernetes_cluster_role" "sidecar_cluster_role" {
  metadata {
    name = "sidecar-injector"
  }

  rule {
    api_groups = [""]
    resources  = ["configmaps"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_service_account" "sidecar_sa" {
  metadata {
    name      = "sidecar-injector"
    namespace = local.namespace
    labels = {
      "kubernetes.io/cluster-service"   = "true"
      "addonmanager.kubernetes.io/mode" = "Reconcile"
    }
  }
  depends_on = [
    kubernetes_namespace.sidecar_ns
  ]
}

resource "kubernetes_cluster_role_binding" "sidecar_cluster_role_binding" {
  metadata {
    name = "sidecar-injector"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.sidecar_cluster_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.sidecar_sa.metadata[0].name
    namespace = local.namespace
  }
  depends_on = [
    kubernetes_cluster_role.sidecar_cluster_role,
    kubernetes_service_account.sidecar_sa
  ]
}

resource "kubernetes_service" "sidecar_srvc" {
  metadata {
    name      = "sidecar-injector-prod"
    namespace = local.namespace
    labels = {
      "k8s-app" = "sidecar-injector"
      "track"   = "prod"
    }
  }
  spec {
    selector = {
      k8s-app = "sidecar-injector"
      "track" = "prod"
    }
    port {
      name        = "https"
      port        = 443
      target_port = "https"
      protocol    = "TCP"
    }

    type = "ClusterIP"
  }
  depends_on = [
    kubernetes_namespace.sidecar_ns
  ]
}

resource "kubernetes_mutating_webhook_configuration" "sidecar_mutating_webhook" {
  metadata {
    name = "sidecar-injector-webhook"
    labels = {
      "k8s-app" = "sidecar-injector"
      "track"   = "prod"
    }
  }
  webhook {
    name           = "injector.tumblr.com"
    failure_policy = "Ignore"
    side_effects   = "None"
    rule {
      api_groups   = [""]
      api_versions = ["v1"]
      operations   = ["CREATE"]
      resources    = ["pods"]
    }
    client_config {
      service {
        name      = "sidecar-injector-prod"
        namespace = local.namespace
        path      = "/mutate"
      }
      ca_bundle = tls_self_signed_cert.sci_ca_cert.cert_pem
    }
    admission_review_versions = ["v1beta1"]
  }
  depends_on = [
    kubernetes_namespace.sidecar_ns
  ]
}
