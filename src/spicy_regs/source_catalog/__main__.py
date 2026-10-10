"""Explicit offline build, query, validation, gap report and export operations."""

import argparse
import json
from pathlib import Path

from .exporter import export
from .importer import build
from .storage import connect


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("build")
    create.add_argument("--corpus", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--config", type=Path)
    create.add_argument("--discovery", type=Path, action="append", default=[])
    publish = commands.add_parser("export")
    publish.add_argument("--index", type=Path, required=True)
    publish.add_argument("--output", type=Path, required=True)
    publish.add_argument("--internal", action="store_true")
    publish.add_argument("--page-size", type=int, default=500)
    for name in ["query", "validate", "gap-report", "matrix"]:
        command = commands.add_parser(name)
        command.add_argument("--index", type=Path, required=True)
        command.add_argument("--collection")
        if name == "query":
            command.add_argument("--limit", type=int, default=100)
            command.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    if args.command == "build":
        result = build(args.corpus, args.output, config=args.config, discovery=args.discovery)
    elif args.command == "export":
        manifest = export(args.index, args.output, internal=args.internal, page_size=args.page_size)
        result = {
            "generation": manifest["generation"],
            "totals": manifest["totals"],
            "manifest": str(args.output / "manifest.json"),
        }
    else:
        db = connect(args.index, readonly=True)
        try:
            params = (args.collection,) if args.collection else ()
            where = " WHERE collection_id=?" if args.collection else ""
            if args.command == "validate":
                result = {
                    "integrity": db.execute("PRAGMA integrity_check").fetchone()[0],
                    "foreignKeys": [list(r) for r in db.execute("PRAGMA foreign_key_check")],
                    "generation": json.loads(
                        db.execute("SELECT value FROM metadata WHERE key='generation'").fetchone()[0]
                    ),
                }
            elif args.command == "query":
                if not 1 <= args.limit <= 5000 or args.offset < 0:
                    parser.error("limit must be 1..5000 and offset must be nonnegative")
                result = [
                    dict(r)
                    for r in db.execute(
                        "SELECT * FROM files" + where + " ORDER BY id LIMIT ? OFFSET ?",
                        (*params, args.limit, args.offset),
                    )
                ]
            elif args.command == "gap-report":
                result = [
                    dict(r)
                    for r in db.execute(
                        "SELECT collection_id,status,count(*) AS items FROM files"
                        + where
                        + " GROUP BY collection_id,status ORDER BY collection_id,status",
                        params,
                    )
                ]
            else:
                inventory_by_route = {}
                for route_id, status, count in db.execute(
                    "SELECT route_id,status,count(*) FROM files WHERE route_id IS NOT NULL GROUP BY route_id,status"
                ):
                    inventory_by_route.setdefault(route_id, {})[status] = count
                relationships_by_route = {}
                policies_by_route = {}
                for kind, data in db.execute(
                    "SELECT kind,data FROM evidence WHERE kind IN ('relationships','planned') ORDER BY id"
                ):
                    evidence = json.loads(data)
                    if kind == "relationships":
                        for route_id in {evidence.get("from_id"), evidence.get("to_id")} - {None}:
                            relationships_by_route.setdefault(route_id, []).append(evidence)
                    elif evidence.get("route_id"):
                        policies_by_route.setdefault(evidence["route_id"], []).append(evidence)
                result = []
                for row in db.execute("SELECT * FROM routes" + where + " ORDER BY id", params):
                    route = json.loads(row["data"])
                    qualified = [
                        json.loads(r[0])
                        for r in db.execute(
                            "SELECT data FROM evidence WHERE kind='periods' AND collection_id=?",
                            (row["collection_id"],),
                        )
                    ]
                    periods = (route.get("periods", []) + [p for p in qualified if p.get("route_id") == row["id"]]) or [
                        {"kind": "unknown", "label": "Unknown", "basis": "no-qualified-period-assertion"}
                    ]
                    for period in periods:
                        relationships = relationships_by_route.get(row["id"], [])
                        result.append(
                            {
                                "collection_id": row["collection_id"],
                                "route_id": row["id"],
                                "period": period,
                                "route": route,
                                "comparison": (
                                    "Scoped relationship evidence recorded; apply only within each stated scope"
                                    if relationships
                                    else "unresolved; no qualified cross-route identity mapping"
                                ),
                                "relationships": relationships,
                                "selection_policies": policies_by_route.get(row["id"], []),
                                "inventory": inventory_by_route.get(row["id"], {}),
                                "holdings_assignment": "Only explicit route_id bindings count; unassigned collection holdings remain unresolved",
                            }
                        )
        finally:
            db.close()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
