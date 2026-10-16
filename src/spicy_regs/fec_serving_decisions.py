"""Portable expressions for the maintained legacy FEC association decisions.

The producer evaluates the small target/header populations with maintained SQL.
Consumers interpret a closed expression vocabulary, without duplicating filing
policy or materializing the financial population. Expressions are data, not code.
"""

import json
import re

VERSION = "fec-row-expressions/1"
PREFIX = "urn:fec:filing:official-fec:openfec-file-number:"


def _key(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")) if isinstance(value, list) else value


def evaluate(program, row):
    """Reference interpreter; null is distinct from empty text and zero."""
    variables = {"row": row}

    def expr(node):
        op, *args = node
        if op == "literal":
            return args[0]
        if op == "var":
            return variables.get(args[0])
        if op == "get":
            value, key = expr(args[0]), expr(args[1])
            return value.get(key) if isinstance(value, dict) and isinstance(key, str) else None
        if op == "if":
            return expr(args[1] if expr(args[0]) is True else args[2])
        if op == "and":
            return all(expr(a) is True for a in args)
        if op == "or":
            return any(expr(a) is True for a in args)
        if op == "not":
            return expr(args[0]) is not True
        values = [expr(a) for a in args]
        if op == "eq":
            return type(values[0]) is type(values[1]) and values[0] == values[1]
        if op == "regex":
            return isinstance(values[0], str) and re.fullmatch(values[1], values[0]) is not None
        if op == "concat":
            return "".join(values) if all(isinstance(v, str) for v in values) else None
        if op == "tuple":
            return values
        if op == "lookup":
            key = _key(values[1])
            return program["lookups"][values[0]].get(key) if isinstance(key, str) else None
        if op == "json":
            try:
                value = json.loads(values[0])
                return value if isinstance(value, dict) else None
            except (ValueError, TypeError):
                return None
        raise ValueError("Unknown FEC expression operator: " + op)

    if program["format"] != VERSION:
        raise ValueError("Unsupported FEC expression version")
    for name, expression in program["bindings"]:
        variables[name] = expr(expression)
    return {name: expr(expression) for name, expression in program["outputs"].items()}


def decision_program(filings, headers, namespaces, *, historical_pin, observations_pin):
    """Reduce only small sides with SQL; retain duplicate/conflicting decisions."""
    import duckdb
    import pyarrow as pa
    from .relationship_views.fec_filing_associations import (
        FILE_NUMBER_MAPPINGS,
        FILE_NUMBER_FILER_FIELDS,
        financial_number_association_sql,
        financial_header_association_sql,
    )
    from .relationship_views.fec_query_views import _NUMBER_TARGET_COLUMNS, _HEADER_COLUMNS

    routes = {
        name: dict(mapper=mapper, evidence=namespaces.get(name), filerField=FILE_NUMBER_FILER_FIELDS[name])
        for name, mapper in FILE_NUMBER_MAPPINGS.items()
    }
    namespace = next(iter(FILE_NUMBER_MAPPINGS))
    groups = {}
    for filing in filings:
        if filing["filing_key"] is not None:
            groups.setdefault(filing["filing_key"], []).append(filing)
    probes = [
        dict(
            record_id=str(i),
            collection_id="probe",
            source_record_id="probe",
            source_sha256="sha256:" + "0" * 64,
            source_authority="official-fec",
            source_locator_json="{}",
            selection_evidence_sha256="sha256:" + "0" * 64,
            source_namespace=namespace,
            mapping_version=FILE_NUMBER_MAPPINGS[namespace],
            report_number=key.removeprefix(PREFIX),
        )
        for i, key in enumerate(groups)
    ]
    targets, header_lookup = {}, {}
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MiB", "max_temp_directory_size": "0B"}) as con:
        con.register(
            "fec_filings",
            pa.Table.from_pylist(filings, schema=pa.schema([(k, pa.string()) for k in _NUMBER_TARGET_COLUMNS])),
        )
        if probes:
            con.register("fec_program_probe", pa.Table.from_pylist(probes))
            rows = (
                con.sql(
                    financial_number_association_sql(
                        "fec_program_probe",
                        historical_pin,
                        {namespace: "sha256:" + "0" * 64},
                        columns=tuple(probes[0]),
                        legacy=True,
                    )
                )
                .to_arrow_table()
                .to_pylist()
            )
            for row in rows:
                key = row["referenced_filing_key"]
                if key is not None:
                    filers = sorted(
                        {r["native_filer_id"] for r in groups[key] if r["native_filer_id"] not in (None, "")}
                    )
                    targets[key] = dict(
                        status=row["association_status"],
                        key=row["filing_key"],
                        count=row["filing_observation_count"],
                        ids=row["filing_observation_ids"],
                        filer=filers[0] if filers else None,
                    )
        for row in headers:
            if row["association_status"] == "resolved_native_filing_key":
                target = targets.get(row["filing_key"])
                if (
                    target is None
                    or target["status"] != "resolved_native_filing_key"
                    or row.get("filing_observation_count") != target["count"]
                    or sorted(row.get("filing_observation_ids") or []) != target["ids"]
                ):
                    raise ValueError("Resolved header witnesses differ from the selected filing population")
        grouped_headers = {}
        for row in headers:
            if row["source_generation_pin"] == historical_pin:
                key = [row[k] for k in ("collection_id", "source_sha256", "source_authority", "header_record_id")]
                grouped_headers.setdefault(_key(key), []).append(row)
        selected = [rows[0] for rows in grouped_headers.values()]
        probes = [
            dict(
                record_id=str(i),
                collection_id=r["collection_id"],
                source_record_id="probe",
                source_sha256=r["source_sha256"],
                source_authority=r["source_authority"],
                filing_header_record_id=r["header_record_id"],
                filing_header_locator_json=r["header_locator_json"],
            )
            for i, r in enumerate(selected)
        ]
        con.register(
            "fec_filing_header_associations",
            pa.Table.from_pylist(headers, schema=pa.schema([(k, pa.string()) for k in _HEADER_COLUMNS])),
        )
        if probes:
            con.register("fec_program_probe", pa.Table.from_pylist(probes))
            rows = (
                con.sql(financial_header_association_sql("fec_program_probe", historical_pin, legacy=True))
                .to_arrow_table()
                .to_pylist()
            )
            for row in rows:
                source = selected[int(row["target_record_id"])]
                key = _key(
                    [source[k] for k in ("collection_id", "source_sha256", "source_authority", "header_record_id")]
                )
                header_lookup[key] = dict(
                    status=row["association_status"],
                    key=row["filing_key"],
                    locator=source["header_locator_json"],
                    recordId=row["association_record_id"],
                    count=source.get("filing_observation_count") if len(grouped_headers[key]) == 1 else None,
                    ids=source.get("filing_observation_ids") if len(grouped_headers[key]) == 1 else None,
                )

    def lit(value):
        return ["literal", value]

    def var(name):
        return ["var", name]

    def get(value, key):
        return ["get", value, lit(key)]

    def field(key):
        return get(var("row"), key)

    def eq(a, b):
        return ["eq", a, b]

    def ne(a, b):
        return ["not", eq(a, b)]

    def choose(condition, yes, no):
        return ["if", condition, yes, no]

    def lookup(name, key):
        return ["lookup", lit(name), key]

    def both(*items):
        return ["and", *items]

    def cases(items, default):
        for condition, value in reversed(items):
            default = choose(condition, value, default)
        return default

    null = lit(None)
    resolved = lit("resolved_native_filing_key")
    route, target, header = (var(name) for name in ("route", "target", "header"))
    valid_number = ["regex", field("report_number"), lit("-?[1-9][0-9]*")]
    number_status = cases(
        [
            (ne(field("source_authority"), lit("official-fec")), lit("unresolved_source_authority")),
            (
                ["or", eq(route, null), ne(field("mapping_version"), get(route, "mapper"))],
                lit("unresolved_native_namespace"),
            ),
            (eq(get(route, "evidence"), null), lit("unresolved_namespace_evidence")),
            (
                ["or", *[eq(field("report_number"), lit(x)) for x in (None, "", "0", "-0")]],
                lit("unresolved_no_file_number"),
            ),
            (["not", valid_number], lit("unresolved_invalid_file_number")),
            (eq(target, null), lit("unresolved_target_not_retained")),
            (ne(get(target, "status"), resolved), get(target, "status")),
            (
                both(
                    ne(var("filer"), null),
                    ne(var("filer"), lit("")),
                    ne(get(target, "filer"), null),
                    ne(var("filer"), get(target, "filer")),
                ),
                lit("unresolved_filer_identity_conflict"),
            ),
        ],
        resolved,
    )
    header_status = cases(
        [
            (eq(header, null), lit("unresolved_header_association_absent")),
            (
                [
                    "regex",
                    get(header, "status"),
                    lit("unresolved_header_(association_ambiguous|policy_version|policy_semantics)"),
                ],
                get(header, "status"),
            ),
            (
                ne(var("headerLocator"), get(header, "locator")),
                lit("unresolved_header_locator_mismatch"),
            ),
        ],
        get(header, "status"),
    )
    context = both(
        *[
            ["regex", field(name), lit("[\\s\\S]+")]
            for name in ("record_id", "collection_id", "source_record_id", "source_authority")
        ],
        *[
            ["regex", field(name), lit("sha256:[0-9a-f]{64}")]
            for name in ("source_sha256", "selection_evidence_sha256")
        ],
        *[eq(get(var("locator"), name), field(name)) for name in ("collection_id", "source_record_id")],
    )
    bindings = [
        ["route", lookup("routes", field("source_namespace"))],
        ["filer", ["get", var("row"), get(route, "filerField")]],
        [
            "headerId",
            choose(
                ne(field("filing_header_record_id"), null), field("filing_header_record_id"), field("header_record_id")
            ),
        ],
        [
            "headerLocator",
            choose(
                ne(field("filing_header_record_id"), null),
                field("filing_header_locator_json"),
                field("header_locator_json"),
            ),
        ],
        [
            "key",
            choose(
                both(
                    valid_number,
                    eq(field("source_authority"), lit("official-fec")),
                    eq(field("mapping_version"), get(route, "mapper")),
                    ne(get(route, "evidence"), null),
                ),
                ["concat", lit(PREFIX), field("report_number")],
                null,
            ),
        ],
        ["target", lookup("targets", var("key"))],
        [
            "header",
            lookup(
                "headers",
                [
                    "tuple",
                    *[field(k) for k in ("collection_id", "source_sha256", "source_authority")],
                    var("headerId"),
                ],
            ),
        ],
        ["locator", ["json", field("source_locator_json")]],
        ["useHeader", ne(var("headerId"), null)],
        ["contextValid", context],
        [
            "status",
            choose(
                var("contextValid"),
                choose(var("useHeader"), header_status, number_status),
                lit("unresolved_source_context"),
            ),
        ],
    ]
    outputs = dict(
        filing_key=choose(eq(var("status"), resolved), choose(var("useHeader"), get(header, "key"), var("key")), null),
        filing_association_status=var("status"),
        filing_link_status=var("status"),
        filing_association_policy=lit("fec-retained-filing-association/1"),
        filing_association_source_generation_pin=lit(observations_pin),
        filing_association_evidence_generation_pin=lit(historical_pin),
        filing_association_target_generation_scope=lit("self"),
        filing_association_target_generation_pin=null,
        filing_association_record_id=choose(var("useHeader"), get(header, "recordId"), null),
        filing_association_referenced_key=choose(
            both(var("contextValid"), ["not", var("useHeader")]), var("key"), null
        ),
        filing_association_target_count=choose(
            var("contextValid"),
            choose(
                var("useHeader"),
                choose(eq(var("headerLocator"), get(header, "locator")), get(header, "count"), null),
                choose(eq(target, null), lit(0), get(target, "count")),
            ),
            null,
        ),
        filing_association_target_record_ids=choose(
            var("contextValid"),
            choose(
                var("useHeader"),
                choose(eq(var("headerLocator"), get(header, "locator")), get(header, "ids"), null),
                choose(eq(target, null), lit([]), get(target, "ids")),
            ),
            null,
        ),
    )
    return dict(
        format=VERSION,
        lookups=dict(routes=routes, targets=targets, headers=header_lookup),
        bindings=bindings,
        outputs=outputs,
    )
