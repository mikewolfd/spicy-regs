# Repair an exact attachment-size refusal

Use `scripts/repair_comment_attachment_group.py` when a completed source plan
has retained accepted rows and explicit size refusals that a reviewed reader
can now interpret. It reads exact central-archive originals. It does not fetch
source records or reread accepted originals.

The request is pinned by full SHA-256. It names the previous recovery ZIP and
its digest/length, complete producer identity, identity-file and plan-receipt
digests, and each exact refused key. Each key includes its failed checkpoint,
observed original digest/length/ETag/version and reviewed normalization
occurrences. The request also names the new runtime, reader, executor and
native-shaper hashes. Prior and supplementary source usage and active time
remain cumulative.

The executor verifies the whole ZIP, bounded member layout, prior resource
receipt and remaining budgets before extraction. It checks the complete plan,
every checkpoint, part and evidence file, and the retained source ledgers. It
reuses resolved parts unchanged. For a refused part, it parses only the
explicit original, proves that all accepted rows remain equal, adds the missing
row and creates separate derived evidence. It stages those parts with the
maintained stage verifier. The original plan retains its original reader
identity; `repair.json` explains which rows came from each producer.

Run from the reviewed checkout and its locked existing environment:

```sh
uv run --no-sync python scripts/repair_comment_attachment_group.py \
  --runtime-ref <exact-commit> \
  --request <request.json> --request-sha256 <full-sha256> \
  --archive <exact-recovery.zip> --raw-root <central-raw-root> \
  --workdir <fresh-owned-directory>
```

The ordinary command requires its code to match the named commit. The optional
`--candidate` mode permits explicitly pinned uncommitted code for local proof;
its producer is marked `candidate_only=true` and cannot publish or enter the
hosted aggregate. After composing and freezing the commit, create fresh
requests and rerun from retained inputs. Charge every preceding supervised
attempt, including successful local candidates and failed startup attempts,
to supplementary active time. Preserve the old attempts. Do not restamp a
candidate result as an exact-commit execution.

`execution.json` is written by the maintained supervisor after worker output
and process closure. A worker result alone is insufficient. Optional
`--publish` uses the existing immutable stage uploader and full readback; its
use belongs to the campaign owner. It does not select a catalog generation.

## Admit repaired stages to the aggregate

The separately SHA-pinned `comment-attachment-repair-stages/1` manifest names
the unchanged source master and reader, exact repair producer and selected
groups. Each group pins `result.json`, `repair.json`, `repair-request.json`,
the completed supervisor `execution.json`, original plan receipt, stage receipt
and checksum. It also states the exact predecessor archive, request, source
keys/digests/lengths and immutable staged Parquet key.

The aggregate workflow accepts `repair_key` and `repair_sha256` together. It
downloads those compact controls through the existing checked staging path,
and obtains every other runner group through its unchanged GitHub controls.
Admission verifies the exact request, predecessor, added rows and both
producers, complete supervisor closure and cumulative resource bounds. Ordinary
groups retain their original master-reader checks. The aggregate still checks
exact whole-group key membership, identity routing and final output values.

Keep repair admission and its pinned controls with the campaign evidence.
Retain prior archives, refused evidence and originals. They explain the
derivation and support recovery; a repaired stage does not replace them.
