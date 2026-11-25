"""Native SEC history and competing fact observations from pinned official JSON."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

SOURCES = "sec_issuer_sources"
FILINGS = "sec_filings"
FACTS = "sec_reported_facts"
SCOPES = "sec_fact_scopes"
PROVENANCE = [
    ("source_sha256", pa.string()),
    ("source_url", pa.string()),
    ("observed_at", pa.string()),
    ("source_pointer", pa.string()),
]
SCHEMAS = {
    SOURCES: pa.schema(
        [
            ("cik", pa.string()),
            ("kind", pa.string()),
            ("continuation", pa.string()),
            ("source_bytes", pa.int64()),
            ("metadata_json", pa.string()),
            *PROVENANCE,
        ]
    ),
    FILINGS: pa.schema(
        [
            ("cik", pa.string()),
            ("accession", pa.string()),
            ("form", pa.string()),
            ("is_amendment", pa.bool_()),
            ("filing_date", pa.string()),
            ("report_date_raw", pa.string()),
            ("acceptance_datetime_raw", pa.string()),
            ("filing_url", pa.string()),
            ("primary_document_url", pa.string()),
            ("raw_row_json", pa.string()),
            *PROVENANCE,
        ]
    ),
    FACTS: pa.schema(
        [
            ("cik", pa.string()),
            ("taxonomy", pa.string()),
            ("concept", pa.string()),
            ("unit", pa.string()),
            ("value_text", pa.string()),
            ("value_status", pa.string()),
            ("period_kind", pa.string()),
            ("period_start", pa.string()),
            ("period_end", pa.string()),
            ("accession", pa.string()),
            ("form", pa.string()),
            ("is_amendment", pa.bool_()),
            ("filed_date", pa.string()),
            ("fiscal_year", pa.int32()),
            ("fiscal_period", pa.string()),
            ("frame", pa.string()),
            ("label", pa.string()),
            ("description", pa.string()),
            ("context_json", pa.string()),
            ("observation_id", pa.string()),
            ("filing_url", pa.string()),
            *PROVENANCE,
        ]
    ),
    SCOPES: pa.schema(
        [
            ("cik", pa.string()),
            ("taxonomy", pa.string()),
            ("concept", pa.string()),
            ("end_from", pa.string()),
            ("end_through", pa.string()),
            ("outcome", pa.string()),
            ("observation_count", pa.int64()),
            *PROVENANCE,
        ]
    ),
}


def _json(value):
    # Decimal values remain exact text. The original JSON and pointer keep their native number spelling.
    return json.dumps(value, default=str, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _provenance(source, pointer):
    return {
        "source_sha256": source["sha256"],
        "source_url": source["url"],
        "observed_at": source["observed_at"],
        "source_pointer": pointer,
    }


def build_tables(selection, concepts, output_dir, *, fact_start, fact_end, store, evidence):
    """Validate selected history completeness before writing any final table.

    One capture per native route. Overlapping repeated accessions refuse rather
    than multiplying filings. All competing fact observations survive unchanged.
    """
    from spicy_docs.sources.sec_edgar import (
        MAX_BYTES,
        cik,
        companyfacts_url,
        fact_observations,
        filing_url,
        iso_date,
        read_companyfacts,
        read_submissions,
        submissions_url,
    )

    if selection.get("format") != "spicy-sec-edgar-captures" or selection.get("version") != 1:
        raise ValueError("Unknown SEC capture selection")
    start, end = map(iso_date, (selection["filing_start"], selection["filing_end"]))
    if start > end or iso_date(fact_start) > iso_date(fact_end):
        raise ValueError("SEC selection window is reversed")
    concepts = [tuple(item) for item in concepts]
    if (
        not concepts
        or any(len(item) != 2 or not all(isinstance(v, str) and v for v in item) for item in concepts)
        or len(set(concepts)) != len(concepts)
    ):
        raise ValueError("Select unique SEC taxonomy/concept pairs")
    pages, parsed, sources = {}, {}, []
    tables = {name: [] for name in SCHEMAS}
    for source in selection["sources"]:
        issuer, kind, continuation = cik(source["cik"]), source["kind"], source["continuation"]
        if issuer != source["cik"] or kind not in {"submissions", "companyfacts"}:
            raise ValueError("Invalid SEC source identity")
        if kind == "companyfacts" and continuation is not None:
            raise ValueError("Companyfacts has no continuation route")
        url = submissions_url(issuer, continuation) if kind == "submissions" else companyfacts_url(issuer)
        key = issuer, kind, continuation
        if (
            key in pages
            or source["url"] != url
            or type(source["bytes"]) is not int
            or not 0 < source["bytes"] <= MAX_BYTES
        ):
            raise ValueError("SEC source repeats or differs from selected route/size")
        pages[key] = source
        with store.open(source["sha256"]) as stream:
            body = stream.read(MAX_BYTES + 1)
        if len(body) != source["bytes"] or "sha256:" + hashlib.sha256(body).hexdigest() != source["sha256"]:
            raise ValueError("SEC original differs from its pinned digest/size")
        value = (
            read_submissions(body, issuer, continuation=continuation)
            if kind == "submissions"
            else read_companyfacts(body, issuer)
        )
        parsed[key] = value
        # Re-retain by exact bytes: the central content store reuses the original object.
        evidence.retain_bytes(
            body,
            stage="sec-edgar-original",
            raw_source=True,
            original_location=url,
            dataset_id="sec-edgar-" + kind,
            native_id=issuer,
            covered_period=f"{start}/{end}",
            source_version=source["sha256"],
            observed_at=source["observed_at"],
        )
        metadata = (
            {**value.metadata, "continuations": list(value.continuations)}
            if not isinstance(value, dict)
            else {k: v for k, v in value.items() if k != "facts"}
        )
        sources.append(
            {
                "cik": issuer,
                "kind": kind,
                "continuation": continuation,
                "source_bytes": len(body),
                "metadata_json": _json(metadata),
                **_provenance(source, ""),
            }
        )
    issuers = sorted({key[0] for key in pages})
    if not issuers:
        raise ValueError("Select SEC originals for at least one issuer")
    for issuer in issuers:
        if (issuer, "submissions", None) not in pages or (issuer, "companyfacts", None) not in pages:
            raise ValueError("SEC issuer requires both submissions and companyfacts captures")
        root = parsed[issuer, "submissions", None]
        required = {
            file["name"]: file for file in root.continuations if file["filingFrom"] <= end and file["filingTo"] >= start
        }
        held = {key[2] for key in pages if key[:2] == (issuer, "submissions") and key[2] is not None}
        if held != set(required):
            raise ValueError("SEC continuation selection is incomplete or outside the declared history window")
        seen = set()
        for name in [None, *sorted(required)]:
            page = parsed[issuer, "submissions", name]
            source = pages[issuer, "submissions", name]
            if name is not None:
                descriptor = required[name]
                if len(page.rows) != descriptor["filingCount"] or any(
                    not descriptor["filingFrom"] <= row["filingDate"] <= descriptor["filingTo"] for row in page.rows
                ):
                    raise ValueError("SEC continuation disagrees with its inventory count/date bounds")
            for index, row in enumerate(page.rows):
                number = row["accessionNumber"]
                if number in seen:
                    raise ValueError("SEC submissions repeats an accession across history pages")
                seen.add(number)
                if start <= row["filingDate"] <= end:
                    tables[FILINGS].append(
                        {
                            "cik": issuer,
                            "accession": number,
                            "form": row["form"],
                            "is_amendment": row["form"].endswith("/A"),
                            "filing_date": row["filingDate"],
                            "report_date_raw": row["reportDate"],
                            "acceptance_datetime_raw": row.get("acceptanceDateTime"),
                            "filing_url": filing_url(issuer, number),
                            "primary_document_url": filing_url(issuer, number, row["primaryDocument"])
                            if row["primaryDocument"]
                            else None,
                            "raw_row_json": _json(row),
                            **_provenance(
                                source, f"/{'filings/recent/' if name is None else ''}accessionNumber/{index}"
                            ),
                        }
                    )
        value = parsed[issuer, "companyfacts", None]
        source = pages[issuer, "companyfacts", None]
        counts = dict.fromkeys(concepts, 0)
        for taxonomy, concept, unit, label, description, row, pointer in fact_observations(value, concepts):
            if not fact_start <= row["end"] <= fact_end:
                continue
            counts[taxonomy, concept] += 1
            fy = row.get("fy")
            if fy is not None and (type(fy) is not int or not 0 < fy < 10000):
                raise ValueError("SEC fiscal year is invalid")
            period_kind = "instant" if "start" not in row else "unknown" if row["start"] is None else "duration"
            tables[FACTS].append(
                {
                    "cik": issuer,
                    "taxonomy": taxonomy,
                    "concept": concept,
                    "unit": unit,
                    "value_text": None if row["val"] is None else str(row["val"]),
                    "value_status": "json_null" if row["val"] is None else "reported",
                    "period_kind": period_kind,
                    "period_start": row.get("start"),
                    "period_end": row["end"],
                    "accession": row["accn"],
                    "form": row["form"],
                    "is_amendment": row["form"].endswith("/A"),
                    "filed_date": row["filed"],
                    "fiscal_year": fy,
                    "fiscal_period": row.get("fp"),
                    "frame": row.get("frame"),
                    "label": label,
                    "description": description,
                    "context_json": _json(row),
                    "observation_id": hashlib.sha256((source["sha256"] + pointer).encode()).hexdigest(),
                    "filing_url": filing_url(issuer, row["accn"]),
                    **_provenance(source, pointer),
                }
            )
        for (taxonomy, concept), count in counts.items():
            exists = concept in value["facts"].get(taxonomy, {})
            tables[SCOPES].append(
                {
                    "cik": issuer,
                    "taxonomy": taxonomy,
                    "concept": concept,
                    "end_from": fact_start,
                    "end_through": fact_end,
                    "outcome": "observed"
                    if count
                    else "no_observations_in_period"
                    if exists
                    else "concept_not_reported",
                    "observation_count": count,
                    **_provenance(source, "/facts"),
                }
            )
    tables[SOURCES] = sources
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    return tuple(write_rows(tables[name], output_dir / (name + ".parquet"), schema) for name, schema in SCHEMAS.items())


def select_fact(connection, *, cik, taxonomy, concept, unit, end, start=None, accession=None):
    """Return one explicit context or an honest missing/competing/null outcome; never sum."""
    from spicy_docs.sources.sec_edgar import cik as normalize_cik, iso_date

    issuer = normalize_cik(cik)
    iso_date(end)
    if start is not None and iso_date(start) > end:
        raise ValueError("SEC query period is reversed")
    scope = connection.execute(
        """SELECT outcome FROM sec_fact_scopes
        WHERE cik=? AND taxonomy=? AND concept=? AND end_from<=? AND end_through>=?""",
        [issuer, taxonomy, concept, end, end],
    ).fetchall()
    if not scope:
        return {"outcome": "not_selected", "observations": []}
    if len(scope) != 1:
        raise ValueError("SEC query spans competing source selections")
    if scope[0][0] != "observed":
        return {"outcome": scope[0][0], "observations": []}
    parameters = [issuer, taxonomy, concept, unit, start, end]
    sql = """SELECT * FROM sec_reported_facts WHERE cik=? AND taxonomy=? AND concept=? AND unit=?
             AND period_start IS NOT DISTINCT FROM ? AND period_end=?"""
    if accession is not None:
        from spicy_docs.sources.sec_edgar import accession as validate_accession

        sql += " AND accession=?"
        parameters.append(validate_accession(accession))
    result = connection.execute(sql + " ORDER BY filed_date, accession, observation_id", parameters)
    names = [column[0] for column in result.description]
    rows = [dict(zip(names, row, strict=True)) for row in result.fetchall()]
    return {
        "outcome": "missing"
        if not rows
        else "competing_observations"
        if len(rows) > 1
        else "reported_null"
        if rows[0]["value_status"] == "json_null"
        else "selected",
        "observations": rows,
    }


def _validate_concepts(concepts):
    pairs = [tuple(item) for item in concepts]
    if (
        not pairs
        or any(len(item) != 2 or not all(isinstance(v, str) and v for v in item) for item in pairs)
        or len(set(pairs)) != len(pairs)
    ):
        raise ValueError("Select unique SEC taxonomy/concept pairs")
    return pairs


def build_bulk_tables(
    submissions_zip,
    companyfacts_zip,
    concepts,
    output_dir,
    *,
    filing_start,
    filing_end,
    fact_start,
    fact_end,
    submissions_sha256,
    companyfacts_sha256,
    observed_at,
    ciks=None,
    batch_size: int = 2_000,
    max_member_bytes: int | None = None,
    max_entries: int | None = None,
    max_nodes: int | None = None,
):
    """Stream SEC bulk ``submissions.zip`` and ``companyfacts.zip`` into the 4 SEC Parquet tables."""
    from tempfile import TemporaryDirectory

    import pyarrow.parquet as pq

    from spicy_docs.sources.sec_edgar import (
        COMPANYFACTS_BULK_URL,
        DEFAULT_BULK_MAX_ENTRIES,
        DEFAULT_BULK_MAX_MEMBER_BYTES,
        DEFAULT_BULK_MAX_NODES,
        SUBMISSIONS_BULK_URL,
        cik,
        fact_observations,
        filing_url,
        iso_date,
        iter_companyfacts_zip,
        iter_submissions_zip,
    )

    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    start, end = map(iso_date, (filing_start, filing_end))
    if start > end or iso_date(fact_start) > iso_date(fact_end):
        raise ValueError("SEC selection window is reversed")
    concepts = _validate_concepts(concepts)
    selected_ciks = None if ciks is None else [cik(item) for item in ciks]
    if selected_ciks is not None and (not selected_ciks or len(selected_ciks) != len(set(selected_ciks))):
        raise ValueError("Declare unique SEC issuer CIKs")

    member_limit = DEFAULT_BULK_MAX_MEMBER_BYTES if max_member_bytes is None else max_member_bytes
    entry_limit = DEFAULT_BULK_MAX_ENTRIES if max_entries is None else max_entries
    node_limit = DEFAULT_BULK_MAX_NODES if max_nodes is None else max_nodes

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)

    with TemporaryDirectory(dir=output_dir) as scratch:
        scratch_dir = Path(scratch)
        writers = {
            name: pq.ParquetWriter(scratch_dir / (name + ".parquet"), schema, compression="zstd")
            for name, schema in SCHEMAS.items()
        }
        buffers: dict[str, list[dict]] = {name: [] for name in SCHEMAS}

        def flush(name: str) -> None:
            if buffers[name]:
                writers[name].write_table(pa.Table.from_pylist(buffers[name], schema=SCHEMAS[name]))
                buffers[name].clear()

        def emit(name: str, row: dict) -> None:
            buffers[name].append(row)
            if len(buffers[name]) >= batch_size:
                flush(name)

        try:
            roots: dict[str, dict] = {}
            seen_continuations: dict[str, dict[str, tuple[int, str, str]]] = {}
            seen_accessions: dict[str, set[str]] = {}

            for member in iter_submissions_zip(
                submissions_zip,
                ciks=selected_ciks,
                max_member_bytes=member_limit,
                max_entries=entry_limit,
                max_nodes=node_limit,
            ):
                issuer = member.issuer_cik
                source_url = f"{SUBMISSIONS_BULK_URL}#{member.member_name}"
                source = {
                    "sha256": submissions_sha256,
                    "url": source_url,
                    "observed_at": observed_at,
                }
                page = member.page
                metadata = (
                    {**page.metadata, "continuations": list(page.continuations)}
                    if member.continuation is None
                    else {}
                )
                emit(
                    SOURCES,
                    {
                        "cik": issuer,
                        "kind": "submissions",
                        "continuation": member.continuation,
                        "source_bytes": member.byte_size,
                        "metadata_json": _json(metadata),
                        **_provenance(source, ""),
                    },
                )
                if member.continuation is None:
                    if issuer in roots:
                        raise ValueError("SEC bulk submissions repeats an issuer root")
                    roots[issuer] = {
                        file["name"]: file
                        for file in page.continuations
                        if file["filingFrom"] <= end and file["filingTo"] >= start
                    }
                else:
                    row_dates = [row["filingDate"] for row in page.rows]
                    seen_continuations.setdefault(issuer, {})[member.continuation] = (
                        len(page.rows),
                        min(row_dates) if row_dates else "",
                        max(row_dates) if row_dates else "",
                    )
                acc_set = seen_accessions.setdefault(issuer, set())
                for index, row in enumerate(page.rows):
                    number = row["accessionNumber"]
                    if number in acc_set:
                        raise ValueError("SEC submissions repeats an accession across history pages")
                    acc_set.add(number)
                    if start <= row["filingDate"] <= end:
                        emit(
                            FILINGS,
                            {
                                "cik": issuer,
                                "accession": number,
                                "form": row["form"],
                                "is_amendment": row["form"].endswith("/A"),
                                "filing_date": row["filingDate"],
                                "report_date_raw": row["reportDate"],
                                "acceptance_datetime_raw": row.get("acceptanceDateTime"),
                                "filing_url": filing_url(issuer, number),
                                "primary_document_url": filing_url(issuer, number, row["primaryDocument"])
                                if row["primaryDocument"]
                                else None,
                                "raw_row_json": _json(row),
                                **_provenance(
                                    source,
                                    f"/{'filings/recent/' if member.continuation is None else ''}accessionNumber/{index}",
                                ),
                            },
                        )

            if orphan := set(seen_continuations) - set(roots):
                raise ValueError(f"SEC bulk submissions contains continuation(s) without issuer root: {sorted(orphan)}")

            for issuer, required in roots.items():
                cont_map = seen_continuations.get(issuer, {})
                for name, descriptor in required.items():
                    if name not in cont_map:
                        raise ValueError("SEC continuation selection is incomplete or outside the declared history window")
                    row_count, min_date, max_date = cont_map[name]
                    if row_count != descriptor["filingCount"] or (
                        row_count > 0
                        and (min_date < descriptor["filingFrom"] or max_date > descriptor["filingTo"])
                    ):
                        raise ValueError("SEC continuation disagrees with its inventory count/date bounds")

            facts_ciks: set[str] = set()
            for member in iter_companyfacts_zip(
                companyfacts_zip,
                ciks=selected_ciks,
                max_member_bytes=member_limit,
                max_entries=entry_limit,
                max_nodes=node_limit,
            ):
                issuer = member.issuer_cik
                facts_ciks.add(issuer)
                value = member.companyfacts
                source_url = f"{COMPANYFACTS_BULK_URL}#{member.member_name}"
                source = {
                    "sha256": companyfacts_sha256,
                    "url": source_url,
                    "observed_at": observed_at,
                }
                emit(
                    SOURCES,
                    {
                        "cik": issuer,
                        "kind": "companyfacts",
                        "continuation": None,
                        "source_bytes": member.byte_size,
                        "metadata_json": _json({k: v for k, v in value.items() if k != "facts"}),
                        **_provenance(source, ""),
                    },
                )
                counts = dict.fromkeys(concepts, 0)
                for taxonomy, concept, unit, label, description, row, pointer in fact_observations(value, concepts):
                    if not fact_start <= row["end"] <= fact_end:
                        continue
                    counts[taxonomy, concept] += 1
                    fy = row.get("fy")
                    if fy is not None and (type(fy) is not int or not 0 < fy < 10000):
                        raise ValueError("SEC fiscal year is invalid")
                    period_kind = "instant" if "start" not in row else "unknown" if row["start"] is None else "duration"
                    emit(
                        FACTS,
                        {
                            "cik": issuer,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "unit": unit,
                            "value_text": None if row["val"] is None else str(row["val"]),
                            "value_status": "json_null" if row["val"] is None else "reported",
                            "period_kind": period_kind,
                            "period_start": row.get("start"),
                            "period_end": row["end"],
                            "accession": row["accn"],
                            "form": row["form"],
                            "is_amendment": row["form"].endswith("/A"),
                            "filed_date": row["filed"],
                            "fiscal_year": fy,
                            "fiscal_period": row.get("fp"),
                            "frame": row.get("frame"),
                            "label": label,
                            "description": description,
                            "context_json": _json(row),
                            "observation_id": hashlib.sha256(
                                (companyfacts_sha256 + "/" + member.member_name + pointer).encode()
                            ).hexdigest(),
                            "filing_url": filing_url(issuer, row["accn"]),
                            **_provenance(source, pointer),
                        },
                    )
                for (taxonomy, concept), count in counts.items():
                    exists = concept in value["facts"].get(taxonomy, {})
                    emit(
                        SCOPES,
                        {
                            "cik": issuer,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "end_from": fact_start,
                            "end_through": fact_end,
                            "outcome": "observed"
                            if count
                            else "no_observations_in_period"
                            if exists
                            else "concept_not_reported",
                            "observation_count": count,
                            **_provenance(source, "/facts"),
                        },
                    )

            if selected_ciks is not None:
                missing = set(selected_ciks) - (set(roots) & facts_ciks)
                if missing:
                    raise ValueError(f"SEC bulk archives omit selected issuer(s): {sorted(missing)}")
            elif not roots or set(roots) != facts_ciks:
                raise ValueError("SEC bulk submissions and companyfacts archives must cover matching issuers")

            for name in SCHEMAS:
                flush(name)
        finally:
            for writer in writers.values():
                writer.close()

        destinations = []
        for name in SCHEMAS:
            dest = output_dir / (name + ".parquet")
            (scratch_dir / (name + ".parquet")).replace(dest)
            destinations.append(dest)
        return tuple(destinations)
