"""Which output columns of a view's SELECT are a dependency table's column, unchanged.

``describe_table`` gives such a column the dictionary meaning of its origin; a
column the SQL computes (an aggregate, CASE, cast, constant, struct field or a
UNION whose branches disagree) shares at most a name with a source column, never
its meaning, so it carries only what its spec declares. Lineage is read from
DuckDB's own parse tree (``json_serialize_sql``) when a view is bound, never
from column names; the serving connection runs no EXPLAIN for it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

#: A source column as (table, column), or None for a column the view defines.
Origin = tuple[str, str] | None
#: A relation's output: its column names in order, each with its origin.
Columns = list[tuple[str, Origin]]


def table_columns(table: str, names: Sequence[str]) -> Columns:
    """A base table's columns, each originating in itself."""
    return [(name, (table, name)) for name in names]


def view_columns_from(lineage: Mapping[str, Sequence[str]]) -> Columns:
    """An installed view's projected columns, as a relation a later view may read."""
    return [(column, (origin[0], origin[1])) for column, origin in lineage.items()]


def column_lineage(connection: Any, select_sql: str, relations: Mapping[str, Columns]) -> dict[str, list[str]]:
    """Map each output column of ``select_sql`` to the ``[table, column]`` it projects unchanged.

    ``relations`` names every table or installed view the SQL may read, with
    that relation's columns and their own origins, so a view over a view keeps
    the base table's lineage. A bare column reference (aliased or not) and a star
    expansion project; everything else is computed and absent from the result.
    A table function or VALUES list without column aliases has unknown columns,
    and an unqualified reference beside one resolves to nothing rather than to a
    guess.
    """
    [(serialized,)] = connection.execute("SELECT json_serialize_sql(?)", [select_sql]).fetchall()
    parsed = json.loads(serialized)
    if parsed.get("error"):
        raise ValueError(f"Cannot read view lineage: {parsed.get('error_message')}")
    [statement] = parsed["statements"]
    lineage: dict[str, list[str]] = {}
    for name, origin in _node_columns(statement["node"], dict(relations)):
        if origin is not None:
            lineage.setdefault(name, list(origin))
    return lineage


def _node_columns(node: dict, scope: dict[str, Columns | None]) -> Columns:
    """The output columns of a query node; ``scope`` maps relation names to columns (None when unknown)."""
    for entry in node.get("cte_map", {}).get("map", []):
        columns = _node_columns(entry["value"]["query"]["node"], scope)
        aliases = entry["value"].get("aliases") or []
        if aliases:
            origins = [origin for _, origin in columns] + [None] * len(aliases)
            columns = list(zip(aliases, origins, strict=False))
        scope = {**scope, entry["key"].lower(): columns}
    if node["type"] == "SET_OPERATION_NODE":
        left, right = _node_columns(node["left"], scope), _node_columns(node["right"], scope)
        return [(name, origin if origin == other else None) for (name, origin), (_, other) in zip(left, right, strict=False)]
    relations = _from_relations(node.get("from_table"), scope)
    unknown = any(columns is None for _, columns in relations)
    output: Columns = []
    for expression in node["select_list"]:
        kind, alias = expression["type"], expression.get("alias") or ""
        if kind == "STAR":
            excluded = {name.lower() for name in expression.get("exclude_list") or []}
            wanted = expression.get("relation_name") or ""
            for name, columns in relations:
                if (not wanted or name == wanted.lower()) and columns is not None:
                    output.extend((col, origin) for col, origin in columns if col.lower() not in excluded)
        elif kind == "COLUMN_REF":
            names = expression["column_names"]
            origin = None
            if len(names) == 2:
                columns = dict(relations).get(names[0].lower())
                origin = _find(columns, names[1])
            elif len(names) == 1 and not unknown:
                found = [_find(columns, names[0]) for _, columns in relations if _find(columns, names[0], present=True)]
                origin = found[0] if len(found) == 1 else None
            output.append((alias or names[-1], origin))
        else:
            output.append((alias or kind, None))
    return output


def _find(columns: Columns | None, name: str, *, present: bool = False) -> Any:
    for column, origin in columns or []:
        if column.lower() == name.lower():
            return True if present else origin
    return False if present else None


def _from_relations(ref: dict | None, scope: dict[str, Columns | None]) -> list[tuple[str, Columns | None]]:
    """``(alias, columns)`` for each relation in FROM order; columns None when the relation's shape is unknown."""
    if not ref:
        return []
    kind, alias = ref["type"], (ref.get("alias") or "").lower()
    renames = ref.get("column_name_alias") or []
    if kind == "JOIN":
        return [*_from_relations(ref["left"], scope), *_from_relations(ref["right"], scope)]
    if kind == "BASE_TABLE":
        name = ref["table_name"].lower()
        columns = scope.get(name)
    elif kind == "SUBQUERY":
        name, columns = alias, _node_columns(ref["subquery"]["node"], scope)
    else:  # TABLE_FUNCTION, EXPRESSION_LIST (VALUES) and anything else DuckDB can read from
        name, columns = alias, None
    if renames:
        origins = [origin for _, origin in columns or []] + [None] * len(renames)
        columns = list(zip(renames, origins, strict=False))
    return [(alias or name, columns)]
