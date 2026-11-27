output "vpc_id" {
  description = "Vultr VPC identifier isolating the cluster private network."
  value       = vultr_vpc.membus.id
}

output "vpc_cidr" {
  description = "Allocated private IPv4 CIDR block for this environment."
  value       = "${var.vpc_v4_subnet}/${var.vpc_v4_subnet_mask}"
}

output "cluster_id" {
  description = "Vultr Kubernetes Engine (VKE) cluster ID."
  value       = vultr_kubernetes.membus.id
}

output "cluster_firewall_group_id" {
  description = "VKE-managed firewall group ID receiving approved admin CIDR rules."
  value       = vultr_kubernetes.membus.firewall_group_id
}

output "cluster_endpoint" {
  description = "VKE Kubernetes API endpoint URL."
  value       = vultr_kubernetes.membus.endpoint
}

output "storage_class_name" {
  description = "Recommended Vultr CSI StorageClass for Flux-managed membus-data-slot-a and membus-data-slot-b PVCs."
  value       = var.storage_class_name
}

output "kubeconfig_raw" {
  description = "Base64-encoded VKE kubeconfig (sensitive; write with chmod 600)."
  value       = vultr_kubernetes.membus.kube_config
  sensitive   = true
}
