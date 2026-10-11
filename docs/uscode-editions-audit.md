# Independent annual U.S. Code qualification

The existing builder produces native edition, structure and section tables with receipts. The independent auditor checks those outputs against every member of the selected original archive. A successful build and a successful qualification are separate results.

## Inputs and scope

Supply a candidate directory, the exact selection manifest used to build it, and an explicit expected annual year. Supported audit year scope covers one selected annual HTML archive from 1994, 1995 or 2000. Year-scope support does not establish that an original archive or installed runtime has passed complete qualification. The manifest year must match the expected year. XML release points and multiple archives require separate qualification work.

The auditor derives expectations from original ZIP bytes, native comments, its own field stack and standard HTML parser events. Maintained components supply output schemas and receipt protocol checks. The auditor never calls the producer, acquisition readers or section readers to derive source expectations.

It checks original member order and identities, literal title metadata and carried publication years, supporting files, duplicate native positions, document pairing, hierarchy, named field spans and nesting, rendered text, null versus empty bodies, decoding issues, every row's receipt and source witnesses. It also compares each table's receipt file with the combined receipts and checks public fields, including recursively decoded JSON, for private locators and credential fields.

## Commands and results

Use the same pinned interpreter and package tree for production and audit. The existing build command takes `--manifest` and `--output`:

```sh
<pinned-python> -I -B -m spicy_regs.sources.uscode_editions \
  --manifest <selection.json> --output <fresh-candidate-directory>

<pinned-python> -I -B -m spicy_regs.sources.uscode_editions_audit \
  <candidate-directory> --selection <selection.json> --annual-year 1995 \
  --output <fresh-qualification.json>
```

The interpreter is the launcher; the Code command has no `--installed-python` flag. Console entry `audit-uscode-editions` is a separate packaging integration. The module command works without that registration.

The library call `audit(candidate, selection, annual_year=1995)` returns a report and writes no files. The command exclusively reserves a fresh report outside the candidate before auditing. It refuses existing paths and aliases to inputs, then writes one success or failure result. Exit zero requires the complete audit to pass. A failure returns nonzero and retains a failure report when the fresh path was reserved. An interrupted or incomplete report never establishes qualification.

The report binds source, selection, rebuild, generation and executing auditor hashes, checked row and content counts, scope and limitations. Retain the report alongside exact command, grant, package/runtime hashes, resource logs and independent result review. The auditor verifies its inputs, outputs and package tree remain unchanged during its checks; it does not supervise operating-system resources or prove a complete runtime closure itself.

## Immutable history and resource limits

Adding this first-party module changes the complete reader package tree used by future builds. That changes future generation identities. Existing frozen candidates and qualification reports remain bound to their original reader trees; the new auditor refuses a candidate from a different tree. Build a fresh candidate under the same new pinned tree before qualifying it. Rewriting a selection's source locator also changes its raw bytes and therefore its generation.

The auditor reads one member and Arrow batches of 32 rows at a time, with a 128 MiB member bound. These bounds do not establish an aggregate memory, disk or thread limit. Installed execution requires separately pinned runtime admission, offline controls, fresh scratch/output locations and approved external resource supervision.

Complete private audits of selected 1994 and 1995 originals provide lineage for this implementation. The 2000 year extension adds policy and synthetic fixture coverage only; its retained converter versions and actual bodies still require complete original-source qualification under a separately admitted successor runtime. They do not qualify a newly installed auditor, other archives, legal interpretation, publisher typography, full history, publication or retirement. Those decisions require their own evidence and authority.
