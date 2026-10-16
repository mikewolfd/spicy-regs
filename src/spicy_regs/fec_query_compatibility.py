"""Keep a published legacy FEC query beside its proven source representation.

This verifies immutable controls and small serving inputs. It neither converts
financial rows nor claims complete restored-original equality. Historical
production parents and witness pins remain part of the retained query entry.
"""

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
import json

from .fec_release_verification import VerifiedFecRelease, digest, load_bound, signature, unchanged, _save

FORMAT = "fec-retained-query-compatibility/1"
SERVING_FORMAT = "fec-retained-query-serving/1"
PRESERVED = {
    "fec_collections": ["collection_id"],
    "fec_source_records": ["collection_id", "source_record_id", "source_sha256"],
}
SMALL_TABLES = (
    "fec_filings",
    "fec_filing_header_associations",
    "fec_filing_definitions",
    "fec_filing_definition_evidence",
    "fec_collection_selection",
)
NUMBER_TABLES = (
    "fec_receipts",
    "fec_intercommittee_transactions",
    "fec_disbursements",
    "fec_independent_expenditures",
    "fec_communication_costs",
)


def _pin(value):
    return {key: value[key] for key in ("logicalId", "artifactDigest")}


def _bound(path):
    path = Path(path).absolute()
    before = signature(path)
    if before["size"] > 8 * 1024**2:
        raise ValueError("Compatibility inputs must be small controls or lookup tables")
    sha = digest(path.read_bytes())
    item = dict(path=str(path), sha256=sha, signature=before)
    unchanged(item)
    return item


def _implementation():
    root = Path(__file__).parent
    return {
        name: digest((root / name).read_bytes())
        for name in (
            "fec_query_compatibility.py",
            "relationship_views/fec_filing_associations.py",
            "relationship_views/fec_query_views.py",
            "fec_versions.py",
            "fec_query_scope.json",
            "fec_serving_decisions.py",
        )
    }


def _controls(observations, query_entry, controls):
    """Validate root identities and membership without opening query payloads."""
    from rulespec_artifacts import stamp_root, MemberDescriptor
    from .sources.publication import parse_index, table_entries

    observations.check()
    values = {name: load_bound(item["path"], item["sha256"]) for name, item in controls.items()}
    if set(values) != {"publication", "artifact", "members"}:
        raise ValueError("Exact retained publication, artifact and members are required")
    index, root, manifest = (values[k] for k in ("publication", "artifact", "members"))
    parse_index(json.dumps(index).encode())
    if index["families"].get("fec-query") != query_entry or "etlReceipts" in query_entry:
        raise ValueError("Query differs from its selected legacy publication")
    if stamp_root(root) != root or _pin(root) != _pin(query_entry):
        raise ValueError("Retained query artifact identity differs")
    spec = root["spec"]
    if (
        root["kind"] != "spicy-regs-rollup-generation"
        or spec["family"] != "fec-query"
        or spec["publicationStatus"] != "complete-family"
        or "etlReceipts" in spec
    ):
        raise ValueError("Only an already published complete legacy query can be retained")
    refs = root["memberManifests"]
    if (
        len(refs) != 1
        or refs[0]["objectKey"] != "members.json"
        or refs[0]["sha256"] != controls["members"]["sha256"]
        or refs[0]["byteSize"] != signature(controls["members"]["path"])["size"]
    ):
        raise ValueError("Query membership differs from the immutable root")
    members = tuple(MemberDescriptor.from_dict(item, path="member") for item in manifest["members"])
    if (
        len(members) != refs[0]["memberCount"]
        or len({m.object_key for m in members}) != len(members)
        or sum(m.byte_size for m in members) != refs[0]["totalMemberByteSize"]
        or sum(m.record_count or 0 for m in members) != refs[0]["totalRecordCount"]
        or table_entries(spec["tables"], members) != query_entry["tables"]
    ):
        raise ValueError("Retained query table membership changed")
    old = observations.root["spec"]["readSnapshot"]["families"]["fec-observations"]
    if _pin(old) != observations.proof["priorPin"]:
        raise ValueError("Observations proof names another historical parent")
    if spec["readSnapshot"]["families"].get("fec-observations") != old:
        raise ValueError("Query was built from a different observations generation")
    expected = {
        key: dict(
            family="fec-observations",
            artifactDigest=old["artifactDigest"],
            sha256=value["sha256"],
            byteSize=value["byteSize"],
        )
        for key, value in old["tables"].items()
    }
    if spec.get("parents") != expected:
        raise ValueError("Query production parent members differ from preserved originals")
    # These fields are copied unchanged by the admitted identity converter.
    # Numeric/list interpretation and generated relationship IDs are excluded.
    from .transforms.fec_identity_context_fields import REGISTRY

    for table, fields in PRESERVED.items():
        rules = REGISTRY[table]
        changed = set(rules["native_scalars"]) | set(rules["native_lists"])
        if (
            set(fields) & changed
            or not set(fields) <= set(rules["subject_fields"])
            or not set(fields) <= set(dict(old["tables"][table + ".parquet"]["columns"]))
        ):
            raise ValueError("Source identity preservation rule changed")
    for item in controls.values():
        unchanged(item)
    return old


def _small_tables(query_entry, inputs):
    import pyarrow.parquet as pq

    if set(inputs) != set(SMALL_TABLES):
        raise ValueError("Complete selected filing and context lookup inputs are required")
    result = {}
    for table, item in inputs.items():
        expected = query_entry["tables"][table + ".parquet"]
        bound = _bound(item["path"])
        if bound != item or item["sha256"] != expected["sha256"] or item["signature"]["size"] != expected["byteSize"]:
            raise ValueError("Small lookup bytes differ from selected query membership")
        parquet = pq.ParquetFile(item["path"])
        if parquet.metadata.num_rows != expected["rows"]:
            raise ValueError("Small lookup row declaration differs")
        result[table] = parquet.read().to_pylist()
        unchanged(item)
    return result


def _namespace_evidence(proofs):
    """Reuse retained publisher FILE_NUM definitions, preserving exact witnesses."""
    from .relationship_views.fec_filing_associations import FILE_NUMBER_MAPPINGS

    scope = json.loads(Path(__file__).with_name("fec_query_scope.json").read_text())
    if set(proofs) != set(FILE_NUMBER_MAPPINGS):
        raise ValueError("Exact retained namespace definitions are required")
    qualified = {}
    for namespace, item in proofs.items():
        proof = load_bound(item["path"], item["sha256"])
        if (
            proof["source_namespace"] != namespace
            or proof["native_field"] not in ("FILE_NUM", "file_num")
            or proof["context_sha256"] != scope["namespace_evidence"][namespace]
            or proof["observedDefinitionLabel"].strip() != "FILE_NUM"
        ):
            raise ValueError("Namespace definition differs from its retained source")
        cells = [cell["text"].strip().lower() for cell in proof["cells"]]
        if any(
            "report id" in cell or "report identifier" in cell or "unique identifier for a submission" in cell
            for cell in cells
        ):
            qualified[namespace] = scope["namespace_evidence"][namespace]
        unchanged(item)
    return qualified


def serving_descriptor(observations, query_entry, old, tables, namespace_evidence):
    """Portable small lookup data for lazy browser and SQL associations.

    Reader adapters must preserve one input row per output and apply decisions
    before filtering, sorting and reverse navigation, not only to visible cells.
    """
    from .relationship_views.fec_filing_associations import FILE_NUMBER_MAPPINGS, FILE_NUMBER_FILER_FIELDS
    from .relationship_views.fec_query_views import _HEADER_COLUMNS, _NUMBER_TARGET_COLUMNS
    from .fec_serving_decisions import decision_program

    old_pin = old["artifactDigest"]
    for table, column in (
        ("fec_filing_header_associations", "source_generation_pin"),
        ("fec_filing_definition_evidence", "witness_generation_pin"),
        ("fec_collection_selection", "source_generation_pin"),
    ):
        if any(row[column] != old_pin for row in tables[table]):
            raise ValueError("Small source witnesses name another historical generation")
    filings = [{k: row[k] for k in _NUMBER_TARGET_COLUMNS} for row in tables["fec_filings"]]
    headers = [{k: row[k] for k in _HEADER_COLUMNS} for row in tables["fec_filing_header_associations"]]
    # A duplicate filing observation is a refusal in the maintained SQL, not a
    # reason to discard rows here. Preserve every version and duplicate.
    modes = {}
    for key, table in query_entry["tables"].items():
        name, columns = key.removesuffix(".parquet"), dict(table["columns"])
        selected = []
        if "filing_header_record_id" in columns or name == "fec_filing_header_associations":
            selected.append("header")
        if name in NUMBER_TABLES:
            selected.append("native-file-number")
        if selected:
            modes[name] = selected
    program = decision_program(
        tables["fec_filings"],
        tables["fec_filing_header_associations"],
        namespace_evidence,
        historical_pin=old_pin,
        observations_pin=observations.pin.artifact_digest,
    )
    logical_columns = [
        [name, "VARCHAR[]" if name.endswith("_record_ids") else "BIGINT" if name.endswith("_count") else "VARCHAR"]
        for name in program["outputs"]
    ]
    return dict(
        format=SERVING_FORMAT,
        queryPin=_pin(query_entry),
        observationsPin=dict(logicalId=observations.pin.logical_id, artifactDigest=observations.pin.artifact_digest),
        observationsProofSha256=digest(Path(observations._bindings[-1]["path"]).read_bytes()),
        historicalObservationsPin=_pin(old),
        preservedSourceColumns=PRESERVED,
        associationPolicy="fec-retained-filing-association/1",
        targetMappingVersion="fec-identity-observations/1",
        targetIdentityVersion="fec-typed-observation/1",
        namespaceEvidence=namespace_evidence,
        numberMappings=FILE_NUMBER_MAPPINGS,
        filerFields=FILE_NUMBER_FILER_FIELDS,
        tableModes=modes,
        filings=filings,
        headers=headers,
        decisionProgram=program,
        logicalColumns=logical_columns,
        limits=[
            "Historical query and witness pins remain unchanged.",
            "No full query conversion or original-value comparison is claimed.",
            "Namespaces without retained report-identifier meaning remain unresolved.",
            "No current filing, amendment replacement, or financial total is qualified.",
        ],
    )


@dataclass(frozen=True)
class VerifiedRetainedFecQuery:
    entry: dict
    observations_pin: object
    serving: dict
    _observations: VerifiedFecRelease
    _bindings: tuple
    _implementation: dict
    _entry_digest: str

    @property
    def descriptor_path(self):
        return Path(self.serving["path"])

    @property
    def descriptor_sha256(self):
        return self.serving["sha256"]

    def check(self):
        self._observations.check()
        if (
            self._implementation != _implementation()
            or digest(json.dumps(self.entry, sort_keys=True).encode()) != self._entry_digest
            or self.serving != self._bindings[-2]
        ):
            raise ValueError("Checked compatibility code or query entry changed")
        for item in self._bindings:
            unchanged(item)


def prepare_retained_query_compatibility(observations, query_entry, *, controls, inputs, definitions, output):
    """Prepare only small lookups; accept no caller-supplied qualification flag."""
    controls = {name: _bound(path) for name, path in controls.items()}
    inputs = {name: _bound(path) for name, path in inputs.items()}
    definitions = {name: _bound(path) for name, path in definitions.items()}
    old = _controls(observations, query_entry, controls)
    serving = serving_descriptor(
        observations, query_entry, old, _small_tables(query_entry, inputs), _namespace_evidence(definitions)
    )
    output = Path(output).absolute()
    output.mkdir(parents=True, exist_ok=True)
    _save(output / "serving.json", serving)
    result = dict(
        format=FORMAT,
        queryEntry=query_entry,
        observationsPin=serving["observationsPin"],
        priorPin=observations.proof["priorPin"],
        controls=controls,
        inputs=inputs,
        definitions=definitions,
        implementation=_implementation(),
        serving=_bound(output / "serving.json"),
        observationsProof=digest(json.dumps(observations.proof, sort_keys=True).encode()),
        checks=dict(
            exactHistoricalParents=True,
            exactQueryMembership=True,
            smallLookupBytes=True,
            sourceIdentityBasis="composed observations preservation evidence",
            financialRowsRewritten=False,
            fullQueryEquivalence=False,
        ),
    )
    _save(output / "compatibility.json", result)
    return result


def verify_retained_query_compatibility(observations, query_entry, basis):
    """Consume the checked retained-query result; payload scans stay unnecessary."""
    if not isinstance(basis, dict) or set(basis) != {"path", "sha256"}:
        raise ValueError("Exact compatibility report path and digest required")
    report = load_bound(basis["path"], basis["sha256"])
    if (
        report.get("format") != FORMAT
        or report["queryEntry"] != query_entry
        or report["implementation"] != _implementation()
        or report["observationsProof"] != digest(json.dumps(observations.proof, sort_keys=True).encode())
        or report["observationsPin"]
        != dict(logicalId=observations.pin.logical_id, artifactDigest=observations.pin.artifact_digest)
        or report["priorPin"] != observations.proof["priorPin"]
    ):
        raise ValueError("Retained query compatibility differs from the selected pair or code")
    old = _controls(observations, query_entry, report["controls"])
    serving = load_bound(report["serving"]["path"], report["serving"]["sha256"])
    expected = serving_descriptor(
        observations,
        query_entry,
        old,
        _small_tables(query_entry, report["inputs"]),
        _namespace_evidence(report["definitions"]),
    )
    if serving != expected:
        raise ValueError("Serving descriptor differs from the exact small inputs and maintained decisions")
    bindings = (
        *report["controls"].values(),
        *report["inputs"].values(),
        *report["definitions"].values(),
        report["serving"],
        dict(path=basis["path"], signature=signature(basis["path"])),
    )
    result = VerifiedRetainedFecQuery(
        deepcopy(query_entry),
        observations.pin,
        deepcopy(report["serving"]),
        observations,
        bindings,
        report["implementation"],
        digest(json.dumps(query_entry, sort_keys=True).encode()),
    )
    result.check()
    return result
