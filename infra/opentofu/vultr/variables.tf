variable "environment" {
  description = "Deployment environment identifier (e.g. dev, staging, prod)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,24}$", var.environment))
    error_message = "environment must be lowercase alphanumeric/hyphen (2..25 chars)."
  }
}

variable "region" {
  description = "Vultr datacenter region slug (e.g. ewr, ord, sjc, ams)."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{3}$", var.region))
    error_message = "region must be a 3-letter lowercase Vultr region code."
  }
}

variable "vpc_v4_subnet" {
  description = "Canonical RFC1918 private VPC IPv4 network prefix (must not overlap across environments)."
  type        = string

  validation {
    condition     = can(regex("^(10\\.[0-9]+\\.[0-9]+\\.[0-9]+|172\\.(1[6-9]|2[0-9]|3[0-1])\\.[0-9]+\\.[0-9]+|192\\.168\\.[0-9]+\\.[0-9]+)$", var.vpc_v4_subnet))
    error_message = "vpc_v4_subnet must be an RFC1918 private IPv4 address (10.0.0.0/8, 172.16.0.0/12, or 192.168.0.0/16)."
  }
}

variable "vpc_v4_subnet_mask" {
  description = "Private VPC IPv4 CIDR prefix length (e.g. 20 for /20)."
  type        = number
  default     = 20

  validation {
    condition     = var.vpc_v4_subnet_mask >= 16 && var.vpc_v4_subnet_mask <= 24
    error_message = "vpc_v4_subnet_mask must be between 16 and 24."
  }
}

variable "k8s_version" {
  description = "Pinned Vultr Kubernetes Engine (VKE) version string."
  type        = string

  validation {
    condition     = can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+\\+[0-9]+$", var.k8s_version))
    error_message = "k8s_version must be a pinned VKE version string (e.g. v1.31.1+1)."
  }
}

variable "ha_controlplanes" {
  description = "Enable Vultr VKE high-availability control plane."
  type        = bool
  default     = true
}

variable "node_pool_plan" {
  description = "Vultr compute plan slug for VKE worker nodes (e.g. vc2-4c-8gb)."
  type        = string
  default     = "vc2-4c-8gb"
}

variable "node_pool_count" {
  description = "Initial worker node count for the VKE pool."
  type        = number
  default     = 3

  validation {
    condition     = var.node_pool_count >= 2 && var.node_pool_count <= 20
    error_message = "node_pool_count must be between 2 and 20 for HA dual-slot scheduling."
  }
}

variable "node_pool_max_nodes" {
  description = "Maximum autoscaler node count for the VKE pool (must be >= node_pool_count)."
  type        = number
  default     = 6

  validation {
    condition     = var.node_pool_max_nodes >= 2 && var.node_pool_max_nodes <= 50
    error_message = "node_pool_max_nodes must be between 2 and 50."
  }
}

variable "storage_class_name" {
  description = "Recommended Vultr CSI SSD/NVMe StorageClass name referenced by Flux slot PVCs."
  type        = string
  default     = "vultr-block-storage"
}

variable "allowed_admin_cidrs" {
  description = "Fail-closed allowlist of IPv4 CIDRs permitted to reach the VKE API (empty by default)."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for c in var.allowed_admin_cidrs : (
        can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+/[0-9]+$", c))
        && can(cidrhost(c, 0))
        && try(tonumber(split("/", c)[1]) >= 8 && tonumber(split("/", c)[1]) <= 32, false)
      )
    ])
    error_message = "allowed_admin_cidrs entries must be valid IPv4 CIDRs with prefix length /8../32 (0.0.0.0/0 and /0 are forbidden)."
  }
}
