# R2 infrastructure (Terraform)

Codifies a selected account's R2 bucket, Iceberg data catalog and cache settings.
The fresh-environment example also creates the public domain and browser CORS
configuration. For the fork, begin with [the account setup](../fork-setup.md).
Account, zone, public domain, browser origins and state endpoint are explicit
inputs; none are taken from the upstream installation.

## Import-first — this adopts EXISTING production resources

When the selected account already has these resources, adopt them into its own
state before applying changes. Import blocks are an inactive example by default;
fresh-account setup should leave them inactive.

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in account_id + zone_id
cp backend.hcl.example backend.hcl             # selected account, private state bucket
# Supply a Cloudflare token through the environment, not a command argument.
# Existing resources only: copy imports.tf.example to imports.tf and fill its IDs.

terraform init -backend-config=backend.hcl
terraform plan     # should show "will import" + NO resource changes
terraform apply    # adopts them into state; no-op on the actual infra
```

A clean plan after import ("3 to import, 0 to add/change/destroy") is the proof
the code matches production. Once imported, delete or leave the `imports.tf`
blocks — they are no-ops thereafter.

The example interpolates account and zone IDs from the selected variables. Fill
its cache ruleset ID from that zone's existing entrypoint. The bucket import ID
includes the `/default` jurisdiction component.

## What it manages (importable)

- `cloudflare_r2_bucket.corpus` — the `spicy-regs` bucket.
- `cloudflare_r2_data_catalog.corpus` — the Iceberg catalog (comments system of record).
- `cloudflare_ruleset.r2_cache` — the edge cache rule, **kept `enabled = false`**.
  Edge-caching parquet corrupts DuckDB's concurrent byte-range reads (see
  `sources/r2.py`, which serves parquet `no-cache`); the resource stays so
  Terraform owns the disabled state and an `apply` can't re-enable the corruption.

## Public domain and CORS

The upstream domain and CORS were managed outside this state. For a fresh
environment, copy `fresh-environment.tf.example` to `fresh-environment.tf` to
create them. For existing settings, check the installed provider's current import
support and resource identity before adopting them; do not recreate live config.

## Guardrails

- `terraform.tfvars` and all `*.tfvars` are gitignored (they hold ids); only
  `*.tfvars.example` is committed. The API token comes from the environment.
- `terraform init -backend=false` and `terraform validate` check configuration
  without reading remote state. A plan needs access to the selected account and
  private state bucket. Inspect the plan before applying changes.
- The cache resource owns the zone's cache-settings ruleset. Preserve any other
  rules in an existing zone; importing it alone does not prove the plan is safe.

## Remote state in R2

State is stored in Cloudflare R2 via the Terraform `s3` backend (`backend.tf`) —
in a **separate, private** bucket, never the public data bucket. The ignored
`backend.hcl` selects the account endpoint and bucket. R2 credentials come from
the environment, never the committed config.

### One-time state setup

```bash
# 1. Create the PRIVATE state bucket (do NOT give it a public domain).
npx wrangler r2 bucket create spicy-regs-tfstate

# 2. Use credentials scoped to the private state bucket.
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"

# 3. Fill backend.hcl with the selected account endpoint and private bucket.
cd deploy/terraform
terraform init -backend-config=backend.hcl

# 4. Inspect the resources addressed by the plan.
terraform plan
```

Use a fresh directory/state for a new fork. Migrating an existing state file is
appropriate only when it describes the same account and resources; preserve
recovery copies during any intentional state migration.

Thereafter every `plan`/`apply` reads and locks state in R2 (the `AWS_*` env vars
must be set). `use_lockfile` puts a lock object in the bucket for the duration of
a run, so two people can't apply at once.

Notes:
- The state bucket **must stay private**. State can contain sensitive attributes;
  a public state bucket would leak them.
- The `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` names are just how the s3 backend
  reads credentials — the values are your **R2** access key + secret, not AWS.
- `*.tfstate*` is gitignored regardless, so state never lands in git even locally.
