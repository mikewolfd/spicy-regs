# PLAN

What a session needs to pick this repository up. Current state only; the
July–August program's disposition is in `docs/disposition.md` and is not
repeated here.

## Where this sits, and the one rule that matters

`origin` is `github.com/civictechdc/spicy-regs`. **Eugene Kim owns `main`
there. We are guests.** `fork` is `github.com/mikewolfd/spicy-regs` and is
Mike's.

- **Do not push to `origin` unless Mike names the branch.** Local `main`
  deliberately tracks `fork/main`, not `origin/main`, so a bare `git push`
  cannot reach Eugene's trunk.
- Two older branches still track `origin/main` rather than a namesake
  (`fix/r2-publish-preflight`, `feat/source-domain-drift-gate-revived`). A bare
  push from either is *addressed at* Eugene's trunk and is refused only because
  `push.default` is unset and therefore `simple`. That is safe by
  configuration, not by intent — name the remote and branch explicitly.
- Two branches have been pushed to `origin`, both on Mike's instruction:
  `docs/disposition` (PR #193) and `feat/court-opinions` (PR #195).

## State

Local `main` at `bf62ac9`, clean, one worktree, suite 1,027 passing.
**18 commits ahead of `fork/main` (`909633a`), pushed nowhere.**
`origin/main` untouched at `1f02a7f`.

Nine PRs are open on `origin`: ours are #181, #182, #183, #193, #194, #195,
#196; #95 and #144 are Eugene's and conflict with main.

## Waiting on Mike — four decisions, none blocked on work

Stated in full, with the measurement that makes each answerable, at the top of
`docs/disposition.md`. In short:

1. **The FCC credential fix upstream, and key rotation.** Two questions, one
   secret. `c6695e0` is committed locally and pushed nowhere. Rotation is
   settled by no code change.
2. **The eight-table registration PR.** `feat/register-eight-tables`
   (`2854262`) is on the fork, unmerged, no PR. Eugene merging #194/#195/#196
   unblocks it; opening the PR is still Mike's call.
3. **`comment` and `restrictReasonType` on the `documents` table.** A schema
   revision; its cost depends on which lane sources it.
4. **Nine uncited `corpora/` scripts.** Default is they stay on the fork.

## Next actionable work

**Add `full_text_xml_url` to the Federal Register ingest.** A person cannot
search inside a rule today, and whoever builds that first will take the pointer
we publish — which is the HTML body, measured over 993 documents to carry
publisher boilerplate on 993 of 993 with a 135-character median passage,
against XML's 0 of 993 and 610. Publishing the wrong pointer exports the defect
downstream.

**Fetch it; do not derive it.** The `html`→`xml` swap on `body_html_url` looks
right and is wrong: tested against the publisher's API across six eras it
matched five and failed on a document published 2000-01-03 carrying a 1999
document number, which has an HTML body and no XML one. A derived backfill
fabricates a 404 for exactly the carried-over documents. XML is otherwise
present on every month probed from 2000 to 2009. The field goes in the source's
field list and the published schema; the backfill is a full-range run of the
existing rollup, which is a publish and therefore Mike's.

## Downstream consumers

`data_dictionary/catalog.json` is a **vendored contract**, not a fetched one.
spicysearch holds a copy pinned by the digest in `catalog.json.sha256`; it
cannot import `spicy_regs`. Changing the file means the consumer must
re-vendor, so bump `CATALOG_FORMAT_VERSION` when the shape changes and say so.
It declares only what this repo *publishes* — it carries no searchability
field, and a test forbids even the words, because whether a class is indexed is
the serving side's fact.

## How to run anything

Everything goes through `uv run`, never a binary from `PATH`. The gate:

```bash
uv run ruff check . && uv run ty check && uv run pytest
uv run spicy-regs-dict check      # descriptions vs schema
uv run spicy-regs-dict generate   # rewrites docs/tables/*.md AND catalog.json
```

`generate` writes the catalog and its digest too, so a contributor cannot leave
them stale by not knowing a third command exists.

## Traps that have cost time here

- **If you can state it without running it, you have not checked it.** Every
  wrong call in this lane came from reasoning about a shape; every correct one
  from executed output. `except OSError` catches no DuckDB read error. A
  matching identifier *shape* is not a matching namespace.
- **A record that a fix was applied is not a check that it worked.** Re-observe
  the thing. Applied, armed and scheduled are three different words.
- **Audit documents against your own recent changes first**, not only against
  the world. The README defect found on 2026-09-07 was introduced that morning
  by the auditor.
- **`grep` exits 1 on zero matches** and will silently break an `&&` chain that
  ends in a commit. `set -o pipefail`; zsh does not word-split unquoted `$var`.
- **State the population before quoting a rate**, and state a scan's reach:
  "clean everywhere I could reach" is a measurement, "clean" is a reassurance.
