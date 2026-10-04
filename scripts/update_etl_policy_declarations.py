"""Generate installed public subject policies from explicit family declarations."""
from __future__ import annotations

import base64
import json
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]


def family_policies():
    from spicy_regs.congress_receipts import policy as congress_policy, ACQUISITION_POLICY
    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.court_receipts import POLICIES as courts
    from spicy_regs.legislative_receipts import policy as legislative_policy, FILE_POLICY
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.scorecards.etl import POLICIES as scorecards
    from spicy_regs.transforms.government_receipts import POLICIES as government
    from spicy_regs.transforms.regulations_receipts import policy as regulations_policy, LEGACY_COLUMNS
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy as identity_policy, REGISTRY
    from spicy_regs.transforms.fec_subject_receipts import dataset_policy as fec_policy
    from spicy_regs.transforms.fec_native_subjects import FIELD_RULES

    result = {}
    def add(policy):
        old = result.get(policy.dataset)
        if old is not None and old.descriptor() != policy.descriptor():
            raise ValueError(f'Family policy conflict: {policy.dataset}')
        result[policy.dataset] = policy
    for policy in (*courts.values(), *scorecards.values(), *government.values(), ACQUISITION_POLICY, FILE_POLICY):
        add(policy)
    for name in INPUT_COLUMNS:
        add(congress_policy(name))
    for name in field_registry():
        add(legislative_policy(name))
    for name in LEGACY_COLUMNS:
        add(regulations_policy(name))
    for name in REGISTRY:
        add(identity_policy(name))
    declarations = json.loads((ROOT / 'data_dictionary/fec_typed_schemas.json').read_text())['tables']
    for name in FIELD_RULES:
        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declarations[name]['arrow_schema_ipc_base64'])))
        add(fec_policy(name, schema))
    return result


def main():
    directory = ROOT / 'src/spicy_regs/etl_policies'
    policies = family_policies()
    for name, policy in policies.items():
        (directory / f'{name}.json').write_text(json.dumps(policy.descriptor(), indent=2) + '\n')
    stale = {p.stem for p in directory.glob('*.json')} - policies.keys()
    if stale:
        raise ValueError(f'Unowned installed policy declarations: {sorted(stale)}')
    from spicy_regs import data_dictionary as dd
    from spicy_regs.contract_types import arrow_schema
    from spicy_regs.native_types import described_schema
    inputs = {name: arrow_schema(contract) for name, contract in dd._contracts().items() if name.startswith("fec_")}
    resource = json.loads((ROOT / "data_dictionary/fec_typed_schemas.json").read_text())["tables"]
    inputs.update({name: pa.ipc.read_schema(pa.BufferReader(base64.b64decode(item["arrow_schema_ipc_base64"])))
                   for name, item in resource.items()})
    import importlib
    for module in ("fec_candidate_observations", "fec_committee_observations"):
        inputs.update(importlib.import_module("spicy_regs.transforms." + module).SCHEMAS)
    from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_SCHEMA
    from spicy_regs.transforms.fec_relationships import SCHEMA as RELATIONSHIP_SCHEMA
    inputs.update(fec_collections=COLLECTION_SCHEMA, fec_source_records=RECORD_SCHEMA,
                  fec_relationships=RELATIONSHIP_SCHEMA)
    payload = {name: {"columns": described_schema(schema), "arrow_schema": base64.b64encode(schema.serialize().to_pybytes()).decode()}
               for name, schema in sorted(inputs.items())}
    (ROOT / "src/spicy_regs/fec_processing_schemas.json").write_text(json.dumps(payload, indent=2) + "\n")
    print(f'Updated {len(policies)} explicit policies')

if __name__ == '__main__':
    main()
