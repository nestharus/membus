# Vultr OpenTofu Module for Dual-Slot Membus A/B Deployments

Provisions Layer 1 cloud infrastructure on Vultr: an isolated RFC1918 VPC, high-availability
Vultr Kubernetes Engine (VKE) cluster, and fail-closed firewall rules attached directly to
VKE's managed firewall group.

Kubernetes namespace, Pod Security `restricted` labels, NetworkPolicies, and the two
independent ReadWriteOnce PVCs (`membus-data-slot-a` and `membus-data-slot-b`) are owned
exclusively by Flux GitOps (`gitops/ab/`), avoiding split IaC/GitOps ownership and avoiding
self-referential Kubernetes provider bootstrap failures.

## Design Invariants

- **Per-Environment Remote State Keys:** `versions.tf` declares an empty `backend "s3" {}`
  block with zero hardcoded buckets, keys, endpoints, or credentials. Always initialize each
  environment with its own `-backend-config` key (for example `envs/staging/membus.tfstate`
  vs. `envs/prod/membus.tfstate`).
- **Non-Overlapping RFC1918 VPC CIDRs:** Pass distinct canonical RFC1918 `vpc_v4_subnet`
  ranges per environment (for example `10.40.0.0/20` for dev, `10.40.16.0/20` for staging,
  `10.40.32.0/20` for prod).
- **Fail-Closed VKE Firewall:** `allowed_admin_cidrs` defaults to `[]`, attaches rules
  directly to `vultr_kubernetes.membus.firewall_group_id`, and rejects `/0`–`/7` wide CIDRs.
- **High-Availability Control Plane:** `ha_controlplanes` defaults to `true`.

## Usage

```bash
export VULTR_API_KEY="<from-secret-manager>"

tofu init \
  -backend-config="bucket=${TF_STATE_BUCKET}" \
  -backend-config="key=envs/${ENV_NAME}/membus-vultr.tfstate" \
  -backend-config="region=${TF_STATE_REGION}" \
  -backend-config="endpoint=${TF_STATE_ENDPOINT}"

tofu plan \
  -var="environment=${ENV_NAME}" \
  -var="region=ewr" \
  -var="vpc_v4_subnet=10.40.16.0" \
  -var="k8s_version=v1.31.1+1"

tofu apply
```

After `tofu apply`, bootstrap Flux with an environment overlay referencing `gitops/ab/`
(which provisions independent `membus-data-slot-a` and `membus-data-slot-b` PVCs on
`var.storage_class_name` and pins immutable `@sha256:` image digests for `membus-slot-a`,
`membus-slot-b`, and `membus-ab-router`).
