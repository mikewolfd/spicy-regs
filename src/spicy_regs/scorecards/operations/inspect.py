"""Inspect every installed scorecard reader without acquisition or extraction."""

import argparse
import ast
from hashlib import file_digest
import json
from pathlib import Path

from spicy_docs.sources.scorecards import ADAPTER_PUBLISHERS, get_adapter

from spicy_regs.scorecards.registry import load_registry


def inspect_readers():
    registry = {source.publisher_id: source for source in load_registry()}
    records = []
    for name, publisher in sorted(ADAPTER_PUBLISHERS.items()):
        adapter = get_adapter(name)
        path = Path(adapter.__file__)
        tree = ast.parse(path.read_text())
        imports = sorted({
            node.module for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
            and ("scorecards" in node.module or node.module in {
                "common", "qualified_observations", "rds_bt50", "votervoice_public",
                "quorum_public", "capwiz_html", "pdf_observations", "workbooks",
            })
        })
        large = [{"function": node.name, "line": node.lineno, "lines": node.end_lineno-node.lineno+1}
                 for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.end_lineno is not None and node.end_lineno-node.lineno+1 > 150]
        with path.open("rb") as stream:
            digest = file_digest(stream, "sha256").hexdigest()
        interfaces = []
        if all(callable(getattr(adapter, method, None)) for method in ("list_scorecards", "acquire_scorecard")):
            interfaces.append("module")
        interfaces.extend(value.__name__ for value in vars(adapter).values()
                          if isinstance(value, type) and value.__module__ == adapter.__name__
                          and all(callable(getattr(value, method, None))
                                  for method in ("list_scorecards", "acquire_scorecard")))
        source = registry.get(publisher)
        records.append(dict(
            adapter=name, publisher_id=publisher, reader_sha256=digest,
            parser_version=adapter.parser_version,
            explicit_interfaces=bool(interfaces), reader_interfaces=interfaces,
            selected_for_live_refresh=bool(source and source.enabled and source.adapter == name),
            qualification_id=source.qualification_id if source else None,
            shared_imports=imports, large_functions=large,
        ))
    return {"scope": "Static installed-reader inspection; not source qualification or live availability",
            "readers": records}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    body = json.dumps(inspect_readers(), indent=2) + "\n"
    if args.output:
        args.output.write_text(body)
    else:
        print(body, end="")
