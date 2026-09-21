# Remote Terraform state in Cloudflare R2 (S3-compatible API).
#
# State lives in a SEPARATE, PRIVATE bucket — never the
# public data bucket, and never public, because state can hold sensitive values.
# Credentials are read from the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
# set to your R2 S3 access key + secret) and are NOT committed here.
#
# The R2-specific bits: region "auto"; the endpoints.s3 R2 URL; use_path_style +
# the skip_* flags + skip_s3_checksum to bypass AWS behaviors R2 doesn't implement
# (skip_s3_checksum is the one people miss — without it writes fail on R2); and
# use_lockfile for S3-native state locking (Terraform >= 1.10), since R2 has no
# DynamoDB for the classic lock table.
terraform {
  backend "s3" {
    # Supply bucket + endpoints through -backend-config=backend.hcl. Keeping
    # account-specific values out of this file lets forks use separate state.
    key    = "deploy/terraform.tfstate"
    region = "auto"

    use_lockfile                = true
    use_path_style              = true
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
  }
}
