resource "yandex_vpc_security_group" "k8s_main" {
  name        = "k8s-main-sg"
  description = "The group rules ensure the basic health of the cluster. Should be applied to the cluster and node groups."
  network_id  = var.network_id

  ingress {
    protocol       = "TCP"
    description    = "Allow health checks from L7 (ALB)"
    v4_cidr_blocks = var.v4_cidrs
    port           = 10501
  }

  ingress {
    protocol          = "TCP"
    description       = "Allow ipv6 health checks from L7 (ALB)"
    port              = 10501
    security_group_id = var.alb_security_group_id
  }

  ingress {
    protocol       = "TCP"
    description    = "Allow health checks from L7 (ALB)"
    v4_cidr_blocks = var.v4_cidrs
    port           = 30081
  }

  ingress { # TODO: figure out how other services (chat-api, ydb-www) live without this rule
    protocol       = "TCP"
    description    = "Allow traffic from ALB"
    v4_cidr_blocks = var.v4_cidrs
    from_port      = 0
    to_port        = 65535
  }

  ingress {
    protocol       = "TCP"
    description    = "Allows health checks from a range of load balancer addresses. It is necessary for the operation of a fault-tolerant cluster and load balancer services."
    v4_cidr_blocks = var.healthchecks_cidrs.v4
    v6_cidr_blocks = var.healthchecks_cidrs.v6
    from_port      = 0
    to_port        = 65535
  }

  ingress {
    protocol          = "ANY"
    description       = "Allows interaction between the master node and the node-node within the security group."
    predefined_target = "self_security_group"
    from_port         = 0
    to_port           = 65535
  }

  ingress {
    protocol       = "ANY"
    description    = "allows the interaction of pod-pod and service-service."
    v4_cidr_blocks = [var.cluster_ipv4_range, var.service_ipv4_range]
    v6_cidr_blocks = [var.cluster_ipv6_range, var.service_ipv6_range]
    from_port      = 0
    to_port        = 65535
  }

  ingress {
    protocol       = "ICMP"
    description    = "Allows debugging ICMP packets from internal subnets."
    v4_cidr_blocks = var.v4_cidrs
  }

  ingress {
    protocol          = "ANY"
    description       = "ipv6 healthcheck"
    from_port         = 0
    to_port           = 65535
    predefined_target = "loadbalancer_healthchecks"
  }

  egress {
    protocol       = "UDP"
    description    = "https://st.yandex-team.ru/CLOUD-118028"
    port           = 53
    v6_cidr_blocks = ["2a02:6b8:0:3400::5005/128"]
  }

  egress {
    protocol       = "ANY"
    description    = "Allows all outgoing traffic. Nodes can contact Yandex Container Registry, Object Storage, etc."
    v4_cidr_blocks = ["0.0.0.0/0"]
    v6_cidr_blocks = ["::/0"]
    from_port      = 0
    to_port        = 65535
  }
}

resource "yandex_vpc_security_group" "k8s_master_whitelist" {
  name        = "k8s-master-whitelist-${var.cluster_name}"
  description = "Only for cluster. Access Kubernetes API from internet. As in https://cloud.yandex.ru/docs/managed-kubernetes/operations/security-groups#examples."
  network_id  = var.network_id

  ingress {
    protocol       = "TCP"
    description    = "Access Kubernetes API on 6443"
    v4_cidr_blocks = var.yandex_nets.ipv4
    v6_cidr_blocks = var.yandex_nets.ipv6
    port           = 6443
  }

  ingress {
    protocol       = "TCP"
    description    = "Access Kubernetes API on 443"
    v4_cidr_blocks = var.yandex_nets.ipv4
    v6_cidr_blocks = var.yandex_nets.ipv6
    port           = 443
  }
}

resource "yandex_vpc_security_group" "k8s_bastion" {
  count = var.bastion.enable ? 1 : 0

  name        = "k8s-bastion"
  description = "Access Kubernetes masters from bastions."
  network_id  = var.network_id

  dynamic "ingress" {
    for_each = var.bastion.enable ? [1] : []
    content {
      protocol       = "TCP"
      description    = "Access Kubernetes API on 443"
      v6_cidr_blocks = var.bastion.cidr
      port           = 443
    }
  }
}
