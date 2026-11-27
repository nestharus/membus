terraform {
  required_version = ">= 1.8.0"

  # Parameterized S3-compatible remote state backend.
  # Supply per-environment bucket, key (e.g. `envs/<env>/membus-vultr.tfstate`),
  # region, and endpoint via `-backend-config` at `tofu init` time.
  # Never hardcode credentials or share one state key across environments.
  backend "s3" {}

  required_providers {
    vultr = {
      source  = "vultr/vultr"
      version = "~> 2.23.0"
    }
  }
}

provider "vultr" {
  # Reads VULTR_API_KEY from environment / secret manager; never stored in source control.
  rate_limit  = 500
  retry_limit = 3
}
