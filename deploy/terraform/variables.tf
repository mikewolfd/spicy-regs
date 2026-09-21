variable "cloudflare_account_id" {
  type        = string
  description = "Cloudflare account ID that owns the R2 bucket."
}

variable "cloudflare_zone_id" {
  type        = string
  description = "Zone ID for the selected data hostname (the cache rule and custom domain live here)."
}

variable "bucket_name" {
  type        = string
  default     = "spicy-regs"
  description = "R2 bucket name (matches R2_BUCKET_NAME)."
}

variable "bucket_location" {
  type        = string
  default     = "ENAM"
  description = "R2 location hint used only when Terraform CREATES the bucket. Ignored for an already-existing (imported) bucket. ENAM = eastern North America."
}

variable "custom_domain" {
  type        = string
  description = "Public custom domain bound to the bucket (matches R2_PUBLIC_URL host)."
}

variable "cors_allowed_origins" {
  type        = list(string)
  description = "Origins allowed to read the corpus from a browser (DuckDB-WASM in spicy-regs-ui)."
}
