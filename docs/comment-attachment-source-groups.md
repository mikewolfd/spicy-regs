# Read one attachment source group on a hosted runner

Use `.github/workflows/read-comment-attachments.yml` for the read phase. It processes one complete source group per job and retains compact recovery files. It does not access or lock the catalog. The existing fill workflow consumes each group's verified staged output afterward.

Build groups with `spicy_regs.pipelines.comment_source_groups` from the frozen manifest and exact retained-source pins. Keep every copy of a comment identity in one group. An identity with any local-held original belongs to a local group; hosted jobs accept only `runner-NNN` groups with no local pins. The grouping receipt records the input, implementation and reader hashes and exact member files.

Before dispatch, place the reviewed `groups.json` and its runner group files under an owned `staging/` prefix in the verified R2 bucket, preserving `runner-NNN/manifest.parquet` and optional `runner-NNN/source-pins.parquet`. Record the exact `groups.json` SHA-256. This is input staging, not selection or publication. Local groups remain local and need their existing central archive. Do not download a complete aggregate onto the workstation.

The manual workflow requires:

- `runtime_ref`: the exact reviewed 40-character repository commit, containing the executor and matching the grouping receipt's reader and grouping implementation hashes;
- `groups_key` and `groups_sha256`: the exact staged descriptor and its hash;
- `groups`: a JSON array such as `["runner-000"]`; qualify one representative group before submitting more;
- optional `resume_run_id`: the earlier workflow run containing this same group's recovery artifact.

The workflow must exist on the repository's default branch before GitHub permits dispatch. Its checkout uses `runtime_ref`, so registration and the executing source remain explicit. The default matrix runs at most four jobs together. Each job uses the executor's fixed key, source-byte, request-attempt, process-tree RSS, local output, free-disk and elapsed-time limits; see `LIMITS`, `SOURCE_BYTES` and `MAX_KEYS` in the maintained script. The job ceiling leaves time after the executor's deadline to upload recovery files. These are monitored bounds, not proof that a new group fits: the process sampler can overshoot between observations, then terminates the owned process tree.

The reader uses the existing conditional/observed source adapter and the shared R2 evidence accessor. Existing R2 originals must match their exact pinned digest and size. External bodies are streamed through the source reader and discarded after parsing; per-key evidence records their observed identity and digest. This does not claim a new complete raw archive. There is no second acquisition owner or automatic fallback from a corrupt retained original.

Actual SDK HTTP attempts, including retries, count against the source request allowance. Streamed source bytes and request counters persist with the checkpoints across resume. The supervisor reuses the maintained process birth/descendant monitor, samples total owned RSS and disk usage, and closes the observed process tree on failure or termination. A resource stop or unresolved key fails the group; it cannot produce a successful incomplete stage.

The only credentials supplied to the read step are `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` and `R2_BUCKET_NAME`. No catalog token, catalog lock or Cloudflare cache-purge credential is supplied. The target account and bucket are verified by the maintained helper. The workflow uses the repository's frozen dependency lock and vendored source-reader package, without changing another release or environment.

The `always()` artifact `comment-attachments-<groups-sha256>-<group>` retains input descriptors, compact plans and parts, per-key evidence, completion checkpoints, source counters, logs and resource records for the workflow's configured retention period. Public repository artifacts are readable by signed-in users with repository read access. This path therefore uses only the attachment profile: no whole comment body, submitter attributes JSON or contact-information part is produced. Do not substitute an older all-fields work directory. Recovery verifies the descriptor, runtime, reader, executor, input members, plan, part and evidence hashes before reuse.

Successful groups upload only the maintained attachment-stage columns and checksum under the existing content-addressed `staging/comment-fields-fill-<digest-prefix>/` route. The immutable uploader never overwrites an existing object; complete byte readback verifies both newly created and already existing members. A partial upload can resume. `result.json` names the resulting `staging_key`, digest and journals prefix for the existing fill workflow (`profile=attachments`). No catalog row or selected publication changes during this read job.

Standard public Linux GitHub runners currently list four CPUs, 16 GB RAM, 14 GB SSD and a six-hour job limit. Actual free space after checkout and installation is still measured by this executor. See [runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners), [Actions limits](https://docs.github.com/en/actions/reference/limits), and [artifact access](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/download-workflow-artifacts).
