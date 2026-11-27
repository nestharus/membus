resource "vultr_vpc" "membus" {
  description    = "membus-${var.environment}-vpc"
  region         = var.region
  v4_subnet      = var.vpc_v4_subnet
  v4_subnet_mask = var.vpc_v4_subnet_mask

  lifecycle {
    precondition {
      condition = (
        can(cidrhost("${var.vpc_v4_subnet}/${var.vpc_v4_subnet_mask}", 0))
        && try(cidrhost("${var.vpc_v4_subnet}/${var.vpc_v4_subnet_mask}", 0) == var.vpc_v4_subnet, false)
      )
      error_message = "vpc_v4_subnet combined with vpc_v4_subnet_mask must form a canonical IPv4 network CIDR."
    }
  }
}

# Attach explicit operator/admin ingress rules directly to VKE's managed firewall group.
# Default `var.allowed_admin_cidrs = []` keeps admin API ingress fail-closed.
resource "vultr_firewall_rule" "admin_https" {
  for_each = toset(var.allowed_admin_cidrs)

  firewall_group_id = vultr_kubernetes.membus.firewall_group_id
  protocol          = "tcp"
  ip_type           = "v4"
  subnet            = split("/", each.value)[0]
  subnet_size       = tonumber(split("/", each.value)[1])
  port              = "6443"
  notes             = "Allow Kubernetes API access from approved operator CIDR"
}
