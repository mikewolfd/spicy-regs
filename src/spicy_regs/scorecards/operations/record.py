"""Advance named coverage only from retained public-file and hosted readback evidence.

Source bodies stay private. Published metadata contains hashes, exact scope
counts and immutable copies of the corresponding source qualifications.
"""

import argparse
from hashlib import file_digest, sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow.parquet as pq

from spicy_regs.scorecards.operations.inventory import read_receipt
from spicy_regs.scorecards.operations.results import query_rows
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.etl_receipts import read_attempts, select_receipts
from spicy_regs.scorecards.etl import POLICIES, SOURCE_NAMES
from spicy_regs.scorecards.subject_shapes import restore_source_row


def document(path):
    raw = path.read_bytes()
    return json.loads(raw), sha256(raw).hexdigest()


def hosted_counts(root, summary, name, generation):
    label = name + "_scope_counts"
    matches = [call for call in summary["calls"] if call["label"] == label and call["tool"] == "query_sql"]
    payload, digest = document(root / (label + ".json"))
    if len(matches) != 1 or matches[0]["response_sha256"] != digest or payload.get("isError"):
        raise ValueError("Hosted scope count response differs from its passed readback")
    body = payload.get("structuredContent")
    if body is None:
        body = json.loads(next(row["text"] for row in payload["content"] if row["type"] == "text"))
    key = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
    expected_sql = f'SELECT "{key}",count(*) AS rows FROM "{name}" GROUP BY "{key}" ORDER BY "{key}"'
    if (
        body.get("error")
        or body.get("truncated") is not False
        or body.get("truncated_cells")
        or body.get("columns") != [key, "rows"]
        or body.get("sql") != expected_sql
        or body.get("publication", {}).get(name, {}).get("artifact_digest") != generation
        or any(pin.get("artifact_digest") != generation for pin in body["publication"].values())
    ):
        raise ValueError("Hosted scope counts do not identify the accepted complete source generation")
    rows = query_rows(body)
    if any(not isinstance(row[key], str) or type(row["rows"]) is not int or row["rows"] < 1 for row in rows):
        raise ValueError("Hosted source scope count shape changed")
    counts = {row[key]: row["rows"] for row in rows}
    if len(counts) != len(rows) or rows != sorted(rows, key=lambda row: row[key]):
        raise ValueError("Hosted source scope counts repeat or are unordered")
    return counts


def publication_proof(readback_root):
    summary, summary_pin = document(readback_root / "summary.json")
    files, files_pin = document(readback_root / "public-files.json")
    index, index_pin = document(readback_root / "public-index.json")
    family = index["families"]["scorecards"]
    generation = family["artifactDigest"]
    if (
        summary.get("status") != "passed"
        or files.get("status") != "passed"
        or files["generation"] != summary["generation"]
        or summary["generation"]["artifactDigest"] != generation
        or set(summary["counts"]) != set(SOURCE_NAMES)
    ):
        raise ValueError("Publication requires a passed complete source-family readback")
    native_names = set(SOURCE_NAMES) - {"scorecard_snapshots"}
    if set(family["tables"]) != {name + ".parquet" for name in native_names}:
        raise ValueError("Publication source-family table membership differs")
    members = {row["key"]: row for row in files["members"]}
    if len(members) != len(files["members"]) or set(members) != set(family["tables"]) | {"etl_receipts.parquet"}:
        raise ValueError("Public source-file membership is incomplete or repeated")
    with duckdb.connect() as connection:
        ExportResources(memory="1GB", threads=1).configure(connection, readback_root / "bookkeeping-spill")
        connection.execute("SET max_temp_directory_size='32GB'")
        counts = {}
        for name in sorted(native_names | {"etl_receipts"}):
            path = readback_root / "members" / (name + ".parquet")
            member = members[path.name]
            declared = family["etlReceipts"] if name == "etl_receipts" else family["tables"][path.name]
            with path.open("rb") as stream:
                digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
            if (
                path.stat().st_size != member["bytes"]
                or digest != member["sha256"]
                or declared["sha256"] != member["sha256"]
                or declared["byteSize"] != member["bytes"]
                or declared["rows"] != member["rows"]
                or pq.ParquetFile(path).metadata.num_rows != member["rows"]
            ):
                raise ValueError("Published member differs from its accepted byte and footer pins")
            connection.read_parquet(str(path)).create_view(name)
            if name == "etl_receipts":
                continue
            counts[name] = hosted_counts(readback_root, summary, name, generation)
            key = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
            local = dict(connection.execute(f'SELECT "{key}",count(*) FROM "{name}" GROUP BY "{key}"').fetchall())
            if local != counts[name] or sum(local.values()) != summary["counts"][name]:
                raise ValueError("Hosted scope counts differ from the pinned public table")
        scopes = {}
        edition_rows = connection.execute("SELECT scorecard_id,publisher_id FROM scorecards").fetchall()
        publishers = dict(edition_rows)
        if len(publishers) != len(edition_rows):
            raise ValueError("Published editions repeat their source identity")
        with TemporaryDirectory(prefix="scorecard-publication-proof-") as temporary:
            selected = Path(temporary) / "snapshots.parquet"
            select_receipts(readback_root / "members/etl_receipts.parquet", selected, dataset="scorecard_snapshots")
            for attempt in read_attempts(
                [selected], POLICIES["scorecard_snapshots"], generation_id=family["etlReceipts"]["generationId"]
            ):
                if attempt["outcome"] != "observed":
                    raise ValueError("Source snapshot publication contains an unfinished acquisition attempt")
                row = restore_source_row("scorecard_snapshots", attempt["processing_fields"])
                scope = row["scorecard_id"]
                publisher = publishers[scope]
                if scope in scopes or row["completeness_status"] != "complete":
                    raise ValueError("Public source snapshots repeat or lack complete acquisition evidence")
                scope_counts = {
                    name: counts[name].get(publisher if name == "scorecard_publishers" else scope, 0)
                    for name in native_names
                }
                scope_counts["scorecard_snapshots"] = 1
                scopes[scope] = dict(publisher_id=publisher, parser_version=row["parser_version"], counts=scope_counts)
    if (
        len(scopes) != summary["counts"]["scorecard_snapshots"]
        or set(scopes) != set(counts["scorecards"])
        or any(set(rows) - set(scopes) for name, rows in counts.items() if name != "scorecard_publishers")
        or {scope["publisher_id"] for scope in scopes.values()} != set(counts["scorecard_publishers"])
    ):
        raise ValueError("Complete snapshots and hosted source populations differ")
    return dict(
        format_version="scorecard-family-publication-readback/1",
        status="passed",
        observed_at=summary["observed_at"],
        source_generation=generation,
        public_member_pins_verified=True,
        hosted_scope_counts_verified=True,
        scopes=dict(sorted(scopes.items())),
        public_members=files["members"],
        hosted_calls=summary["calls"],
        input_pins=dict(
            summary_sha256=summary_pin,
            public_files_sha256=files_pin,
            public_index_sha256=index_pin,
            preparation_sha256=summary["preparation_sha256"],
            publication_receipt_sha256=summary["publication_receipt_sha256"],
        ),
        boundary="Passed readback of named complete source renditions; remaining editions and scheduled refresh remain separate",
    )


def promote(directory, proof):
    ledger, _ = document(directory / "integration_qualifications.json")
    previous, _ = document(directory / "integration_publications.json")
    records = {record["scorecard_id"]: record for record in previous["editions"]}
    proposals = []
    for record in ledger["editions"]:
        if record["scorecard_id"] not in proof["scopes"]:
            continue
        qualified = read_receipt(directory, record)
        if qualified["format_version"] == "scorecard-integration-published-observation/1":
            continue
        if qualified["format_version"] == "scorecard-integration-published-observation/2":
            qualified, _ = document(directory / qualified["source_qualification"])
        if proof["scopes"][record["scorecard_id"]] != dict(
            publisher_id=record["publisher_id"], parser_version=qualified["parser_version"], counts=qualified["counts"]
        ):
            raise ValueError("Qualified scope differs from its complete public readback: " + record["scorecard_id"])
        proposals.append((record, qualified))
    target = directory / "publications" / proof["source_generation"].removeprefix("sha256:")
    target.mkdir(parents=True, exist_ok=True)

    def save(value, path):
        raw = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()
        if path.exists() and path.read_bytes() != raw:
            raise ValueError("Retain prior publication evidence instead of overwriting a different observation")
        path.write_bytes(raw)
        return str(path.relative_to(directory)), sha256(raw).hexdigest()

    proof_path, proof_pin = save(proof, target / "readback.json")
    for record, qualification in proposals:
        raw = (json.dumps(qualification, indent=2, ensure_ascii=False) + "\n").encode()
        qualification_path, qualification_pin = save(qualification, target / (sha256(raw).hexdigest() + ".json"))
        receipt = dict(
            format_version="scorecard-integration-published-observation/2",
            publisher_id=record["publisher_id"],
            scorecard_id=record["scorecard_id"],
            parser_version=qualification["parser_version"],
            counts=qualification["counts"],
            source_qualification=qualification_path,
            source_qualification_sha256=qualification_pin,
            public_readback=proof_path,
            public_readback_sha256=proof_pin,
            observed_source_generation=proof["source_generation"],
        )
        relative, digest = save(receipt, target / (sha256(record["scorecard_id"].encode()).hexdigest() + ".json"))
        published = dict(
            publisher_id=record["publisher_id"], scorecard_id=record["scorecard_id"],
            state="published", receipt=relative, receipt_sha256=digest,
        )
        read_receipt(directory, published)
        records[record["scorecard_id"]] = published
    readers = dict(previous["readers"])
    for record, _ in proposals:
        readers[record["publisher_id"]] = ledger["readers"][record["publisher_id"]]
    result = dict(schema_version="1", editions=[records[key] for key in sorted(records)], readers=readers)
    (directory / "integration_publications.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readback", type=Path, required=True)
    parser.add_argument("--directory", type=Path, default=Path("docs/research/scorecards/work/integration"))
    args = parser.parse_args(argv)
    result = promote(args.directory, publication_proof(args.readback))
    print(json.dumps(dict(published_observations=len(result["editions"]))))


if __name__ == "__main__":
    main()
