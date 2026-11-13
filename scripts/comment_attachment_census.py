"""Count attachment fields/types in the exact checked reread stage."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from spicy_docs.schemas.regulations import COMMENT_ATTACHMENT_METADATA
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines.comment_fields_evidence import file_sha256
from spicy_regs.pipelines.comment_fields_groups import _input, _relation


def census(stage, output):
    if output.exists():
        raise FileExistsError(output)
    reads, receipt = _input(stage)
    expected = {"attachment_id": "text", **{key: kind for key, (_, kind) in COMMENT_ATTACHMENT_METADATA.items()}}
    with TemporaryDirectory(prefix=".attachment-census-", dir=output.parent) as spill, duckdb.connect() as con:
        ExportResources(memory="384MB", threads=1).configure(con, Path(spill))
        con.execute("SET max_temp_directory_size='512MB'")
        relation = _relation(reads)
        stats = con.execute(f"""
            SELECT count(*), count(attachments_json), count(*) FILTER (WHERE attachments_json IS NOT NULL
                  AND json_type(attachments_json) <> 'ARRAY'), sum(json_array_length(attachments_json))
            FROM {relation}
        """).fetchall()[0]
        rows, nonnull, nonarray, count = stats
        if rows != receipt["rows"] or nonarray:
            raise ValueError("Staged attachment rows or list shapes differ from the checked receipt")
        columns = []
        for field, kind in expected.items():
            path = f"'$.{field}'"
            value_type = f"json_type(item, {path})"
            allowed = f"{value_type} = 'VARCHAR'"
            if kind == "integer":
                allowed = f"{value_type} IN ('BIGINT', 'UBIGINT') AND try_cast(json_extract(item, {path}) AS BIGINT) IS NOT NULL"
            if kind == "text array":
                allowed = (f"CASE WHEN {value_type} = 'ARRAY' THEN "
                           f"list_has_all(['VARCHAR'], list_transform(json_extract(item, {path})::JSON[], "
                           "entry -> coalesce(json_type(entry), 'NULL'))) ELSE false END")
            columns.extend([
                f"count(*) FILTER (WHERE NOT json_exists(item, {path}))",
                f"count(*) FILTER (WHERE {value_type} = 'NULL')",
                f"count(*) FILTER (WHERE {allowed})",
            ])
        # A streaming unnest avoids a whole-stage join on the JSON strings.
        raw = con.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE json_type(item) <> 'OBJECT'), {', '.join(columns)}
            FROM (SELECT unnest(json_extract(attachments_json, '$[*]')) AS item FROM {relation})
        """).fetchall()[0]
    entries, nonobjects, *metrics = raw
    if nonobjects or entries != (count or 0) or file_sha256(reads) != receipt["file"]["sha256"]:
        raise ValueError("Staged attachment entries or digest differ from the checked input")
    measured = {}
    for field, at in zip(expected, range(0, len(metrics), 3), strict=True):
        missing, null, valid = metrics[at:at + 3]
        measured[field] = {"type": expected[field], "missing": missing, "null": null, "valid": valid,
                           "wrong_type": entries - missing - null - valid}
    result = {
        "staged_file_sha256": receipt["file"]["sha256"], "source_groups_sha256": receipt["master_source_groups_sha256"],
        "observed_comment_copies": rows, "copies_with_attachment_lists": nonnull, "attachment_entries": entries,
        "fields": measured, "source_gaps": receipt.get("source_gaps"),
    }
    output.write_text(json.dumps(result, indent=2) + "\n")
    if any(stats["wrong_type"] for stats in measured.values()):
        raise ValueError(f"Attachment type census found a wrong type; see {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("output", type=Path)
    census(**vars(parser.parse_args()))
