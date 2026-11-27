resource "vultr_kubernetes" "membus" {
  region           = var.region
  label            = "membus-${var.environment}-vke"
  version          = var.k8s_version
  vpc_id           = vultr_vpc.membus.id
  enable_firewall  = true
  ha_controlplanes = var.ha_controlplanes

  node_pools {
    node_quantity = var.node_pool_count
    plan          = var.node_pool_plan
    label         = "membus-${var.environment}-workers"
    auto_scaler   = true
    min_nodes     = var.node_pool_count
    max_nodes     = var.node_pool_max_nodes

    labels = {
      "membus.oulipoly.io/environment" = var.environment
      "membus.oulipoly.io/workload"    = "membus-ab"
    }
  }

  lifecycle {
    precondition {
      condition     = var.node_pool_max_nodes >= var.node_pool_count
      error_message = "node_pool_max_nodes must be greater than or equal to node_pool_count."
    }
  }
}
