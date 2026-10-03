"""Build bounded, source-grounded proposal fixtures; never acquire or publish data.

Research keys identify checked examples only. A complete fixture has every selected
observation and required reference; it is not a complete source-edition snapshot.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = Path(__file__).with_name("sample_bundles.json")
SCHEMA = json.loads((ROOT / "proposed_schema.json").read_text())
TABLES = {table["name"]: table for table in SCHEMA["tables"]}
CAPTURES = {
    item["capture_id"]: item
    for filename in (
        ROOT / "capture_receipts.json",
        ROOT / "work/samples/capture_additions.json",
    )
    for item in json.loads(filename.read_text())["captures"]
}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def period(text, path, **context):
    return {
        "occurrence_id": "source-period:" + text,
        "period_text": text,
        "kind": "explicit",
        "source_path": path,
        **context,
    }


def reference(key, text, kind, path, **context):
    return {"occurrence_id": key, "citation_text": text, "kind": kind, "source_path": path, **context}


class Bundle:
    def __init__(self, sample_id, scope, limits):
        self.data: dict[str, Any] = {
            "sample_id": sample_id,
            "fixture_only": True,
            "fixture_scope": scope,
            "production_snapshot": False,
            "identity_policy": "Checked research-local semantic keys; no production stability qualification is asserted.",
            "limits": limits,
            "tables": {name: [] for name in TABLES},
            "snapshot_captures": {},
            "literal_assertions": [],
            "corroborating_observations": [],
        }
        self.edition = self.snapshot = self.default_capture = None

    def row(self, table, capture, path, **values):
        unknown = set(values) - set(TABLES[table]["fields"])
        assert not unknown, (table, unknown)
        assert capture in CAPTURES, capture
        receipt = CAPTURES[capture]
        assert receipt.get("http_status") == 200 and receipt.get("capture_complete")
        row = dict.fromkeys(TABLES[table]["fields"])
        if "scorecard_id" in row:
            row["scorecard_id"] = self.edition
        if "snapshot_id" in row:
            row["snapshot_id"] = self.snapshot
        row.update(capture_id=capture, source_url=receipt["resolved_url"], source_path=path)
        row.update(values)
        assert all(value is None or isinstance(value, str) for value in row.values())
        self.data["tables"][table].append(row)
        return row

    def literal(self, table, row, field):
        self.data["literal_assertions"].append(
            {
                "table": table,
                "key": [row[key] for key in TABLES[table]["key"]],
                "field": field,
                "expected": row[field],
            }
        )

    def edition_rows(
        self,
        publisher,
        name,
        edition_key,
        title,
        label,
        chamber,
        captures,
        selection_rule,
        *,
        year=None,
        congress=None,
        session=None,
        periods=None,
    ):
        self.edition = f"fixture:{publisher}:{edition_key}"
        self.snapshot = self.edition + ":bounded-snapshot-v1"
        self.default_capture = captures[0][0]
        capture = self.default_capture
        time = CAPTURES[capture]["observed_at"]
        self.row(
            "scorecard_publishers",
            capture,
            "Publisher attribution on selected source",
            publisher_id=publisher,
            name=name,
            observed_at=time,
        )
        self.row(
            "scorecards",
            capture,
            "Selected edition label and fixture scope",
            publisher_id=publisher,
            series_id=f"fixture:{publisher}:federal",
            publisher_name_text=name,
            title=title,
            edition_label_text=label,
            chamber_scope_text=chamber,
            year_text=year,
            congress_text=congress,
            session_text=session,
            periods_json=encoded(periods) if periods else None,
            observed_at=time,
        )
        capture_ids = list(dict.fromkeys(capture_id for capture_id, _, _ in captures))
        self.data["snapshot_captures"][self.snapshot] = capture_ids
        self.row(
            "scorecard_snapshots",
            capture,
            "Research fixture closure, not a publisher completeness claim",
            observed_at=time,
            capture_ids_json=encoded(capture_ids),
            parser_version="source-grounded-fixture-builder-v1",
            completeness_status="complete",
            completeness_rule="Complete fixture closure only: all selected bounded observations and required references are present. No source-edition completeness or production acceptance is asserted.",
            source_declared_counts_json=None,
            evidence_policy="hash_only",
            capture_roles_json=encoded(
                [{"capture_id": key, "role": role, "field_groups": groups} for key, role, groups in captures]
            ),
            rendition_selection_rule=selection_rule,
            identity_rule_version="research-semantic-keys-v1",
        )
        return self

    def metric(self, key, name, capture, path, **values):
        return self.row("scorecard_metrics", capture, path, metric_id=key, name=name, **values)

    def member(self, key, name, capture, path, **values):
        return self.row("scorecard_members", capture, path, publisher_member_key=key, member_name=name, **values)

    def rating(self, metric, member, value, capture, path, **values):
        row = self.row(
            "scorecard_member_ratings",
            capture,
            path,
            metric_id=metric,
            publisher_member_key=member,
            value_text=value,
            **values,
        )
        self.literal("scorecard_member_ratings", row, "value_text")
        return row

    def finish(self):
        for snapshot in self.data["tables"]["scorecard_snapshots"]:
            counts = {
                name: sum(row.get("scorecard_id") == snapshot["scorecard_id"] for row in rows)
                for name, rows in self.data["tables"].items()
                if name != "scorecard_publishers"
            }
            snapshot["parsed_counts_json"] = encoded(counts)
        return self.data


def afl_bundle():
    b = Bundle(
        "afl_cio:2025:bounded-schema",
        "One House member, annual/lifetime ratings, one letter result and two independently identified dated H.R. 4 items.",
        [
            "Workbook membership is complete in the research capture, but this fixture deliberately selects one member and three items.",
            "No historical lifetime item ledger, official-action comparison or score reproduction is asserted.",
            "No standalone numeric member adjustment is observed or fabricated.",
            "The identical H.R. 4 workbook headers do not provide a proven column-to-item URL crosswalk. Their dated item records remain separate from member-result facts.",
        ],
    )
    b.edition_rows(
        "afl_cio",
        "AFL-CIO",
        "2025",
        "Legislative Scorecard",
        "2025",
        "House",
        [
            (
                "afl-house-2025-xlsx",
                "primary",
                ["member_identity", "metric_headers", "rating_values", "member_item_cells"],
            ),
            ("afl2025-house-item-07", "primary", ["hr4_june_item"]),
            ("afl2025-house-item-04", "primary", ["hr4_july_item"]),
            ("afl2025-house-item-12", "primary", ["letter_item", "letter_semantics"]),
            ("afl-2025-method", "primary", ["edition_context"]),
        ],
        "Workbook strings are primary for rating and result cells. Selected item pages supply distinct dates, item URLs and the explicit letter encoding explanation; their paginated member grids are not used.",
        year="2025",
    )
    method = "fixture:letter-encoding"
    b.row(
        "scorecard_methodologies",
        "afl2025-house-item-12",
        "Article note immediately after position description",
        methodology_id=method,
        chamber_text="House",
        methodology_text='a "no" vote indicates the person signed the letter protesting the firing of Wilcox',
        disclosure_status="explicit",
        methodology_url=CAPTURES["afl2025-house-item-12"]["resolved_url"],
    )
    b.metric("fixture:annual", "Score", "afl-house-2025-xlsx", "Worksheet!E1", chamber_text="House", period_text="2025")
    b.metric("fixture:lifetime", "Lifetime Score", "afl-house-2025-xlsx", "Worksheet!F1", chamber_text="House")
    member = "fixture:house:AK:0:nicholas-j-begich-iii"
    b.member(
        member,
        "Rep. Nicholas J. Begich III",
        "afl-house-2025-xlsx",
        "Worksheet!A2:D2",
        chamber_text="House",
        state="AK",
        district="0",
        party="Republican",
    )
    b.rating("fixture:annual", member, "0%", "afl-house-2025-xlsx", "Worksheet!E2")
    b.rating("fixture:lifetime", member, "0%", "afl-house-2025-xlsx", "Worksheet!F2")
    for capture, key, date, column in [
        ("afl2025-house-item-07", "fixture:hr4-june-12", "June 12, 2025", None),
        ("afl2025-house-item-04", "fixture:hr4-july-18", "July 18, 2025", None),
        ("afl2025-house-item-12", "fixture:wilcox-letter", "February 13, 2025", "G"),
    ]:
        letter = column == "G"
        title = (
            "Letter to President Trump demanding reinstatement of NLRB Member Gwynn Wilcox"
            if letter
            else "H.R. 4, Rescissions Act of 2025"
        )
        path = "Article heading and date"
        item = b.row(
            "scorecard_items",
            capture,
            path,
            item_id=key,
            publisher_item_id=CAPTURES[capture]["resolved_url"],
            title=title,
            item_kind_text="letter" if letter else None,
            item_date_text=date,
            chamber_text="House",
            bill_citation_text=None if letter else "H.R. 4",
            references_json=encoded(
                [] if letter else [reference("source-bill", "H.R. 4", "bill", path, bill_citation_text="H.R. 4")]
            ),
        )
        b.literal("scorecard_items", item, "item_date_text")
        b.literal("scorecard_items", item, "publisher_item_id")
        if not letter:
            continue
        participation = "fixture:annual-workbook-inclusion"
        b.row(
            "scorecard_metric_items",
            "afl-house-2025-xlsx",
            f"Worksheet!{column}1 and E1",
            metric_id="fixture:annual",
            item_id=key,
            participation_id=participation,
            methodology_id=method if letter else None,
        )
        result = b.row(
            "scorecard_member_item_results",
            "afl-house-2025-xlsx",
            f"Worksheet!{column}2",
            result_id="fixture:workbook-cell",
            item_id=key,
            publisher_member_key=member,
            metric_id="fixture:annual",
            participation_id=participation,
            result_text="◯",
        )
        b.literal("scorecard_member_item_results", result, "result_text")
    b.data["corroborating_observations"].append(
        {
            "capture_id": "afl2025-house-item-12",
            "source_path": "Letter explanatory note",
            "observation": "The publisher says the template passed/failed result does not apply because this measure is a letter. Its raw no encoding does not establish an official vote.",
        }
    )
    b.data["corroborating_observations"].append(
        {
            "capture_id": "afl-house-2025-xlsx",
            "source_path": "Worksheet!M1, P1, M2 and P2",
            "header_literal": "H.R. 4, Rescissions Act of 2025 (H.R. 4)",
            "member_cell_literals": {"M2": "ｘ", "P2": "ｘ"},
            "observation": "Two source columns are retained as a duplicate-title challenge. The workbook contains no hyperlinks, and these equal member cells cannot establish which dated source item each column represents. The fixture creates no H.R. 4 metric participation or member-result join.",
        }
    )
    return b.finish()


def lcv_bundle():
    b = Bundle(
        "lcv:2025:bounded-schema",
        "Katie Britt annual/lifetime CSV cells and an Alan Armstrong member occurrence with absent annual rating.",
        [
            "The source export includes both chambers despite chamber query selection.",
            "An absent annual cell creates no invented annual metric value.",
            "No complete member-item ledger is claimed.",
        ],
    )
    b.edition_rows(
        "lcv",
        "League of Conservation Voters",
        "2025",
        "National Environmental Scorecard",
        "2025",
        "Senate",
        [
            ("lcv-2025-senate-csv", "primary", ["members", "rating_values", "metric_headers"]),
            ("lcv-2025-senate-html", "corroboration", ["members", "rating_values", "metric_headers"]),
            ("lcv-method", "primary", ["methodology"]),
        ],
        "CSV is authoritative for these member/rating rows. HTML 0%/2% and N/A corroborate CSV 0/2 and na without replacing the CSV strings.",
        year="2025",
    )
    b.metric(
        "fixture:annual",
        "Year Score",
        "lcv-2025-senate-csv",
        "CSV record2, Year Score header",
        chamber_text="Senate",
        period_text="2025",
    )
    b.metric(
        "fixture:lifetime",
        "Lifetime Score",
        "lcv-2025-senate-csv",
        "CSV record2, Lifetime Score header",
        chamber_text="Senate",
    )
    for key, name, state, rownum, url in [
        ("fixture:lcv-url:katie-britt", "Katie Britt", "Alabama", 4, "https://www.lcv.org/moc/katie-britt/"),
        (
            "fixture:lcv-url:alan-armstrong",
            "Alan Armstrong",
            "Oklahoma",
            111,
            "https://www.lcv.org/moc/alan-armstrong/",
        ),
    ]:
        b.member(
            key,
            name,
            "lcv-2025-senate-csv",
            f"CSV record{rownum} with preceding state heading",
            publisher_member_id=url,
            chamber_text="Senate",
            state=state,
            district="",
            party="R",
        )
    b.rating(
        "fixture:annual",
        "fixture:lcv-url:katie-britt",
        "0",
        "lcv-2025-senate-csv",
        "CSV record4, Year Score",
        value_number="0",
    )
    b.rating(
        "fixture:lifetime",
        "fixture:lcv-url:katie-britt",
        "2",
        "lcv-2025-senate-csv",
        "CSV record4, Lifetime Score",
        value_number="2",
    )
    b.rating(
        "fixture:lifetime",
        "fixture:lcv-url:alan-armstrong",
        "na",
        "lcv-2025-senate-csv",
        "CSV record111, Lifetime Score",
    )
    b.data["corroborating_observations"].extend(
        [
            {
                "capture_id": "lcv-2025-senate-html",
                "source_path": "Katie Britt card / 2025 Score",
                "literal": "0%",
                "primary_literal": "0",
            },
            {
                "capture_id": "lcv-2025-senate-html",
                "source_path": "Alan Armstrong card",
                "literal": "N/A",
                "observation": "Annual score control is absent; CSV annual cell is empty. Member presence is retained independently.",
            },
        ]
    )
    return b.finish()


def nea_bundle():
    b = Bundle(
        "nea:119:bounded-schema",
        "Pete Ricketts published grade and one disclosed Senate amendment item with multiple literal references.",
        [
            "Underlying leadership/accessibility contributions are undisclosed; no synthetic item-result rows are created.",
            "An item listing does not itself link this member to an observed item result.",
        ],
    )
    b.edition_rows(
        "nea",
        "National Education Association",
        "119-2025",
        "Legislative Report Card",
        "119th Congress (2025)",
        "Senate",
        [
            ("nea-119-html", "primary", ["members", "grades", "grade_metric"]),
            ("nea-119-pdf", "corroboration", ["members", "grades", "grade_metric"]),
            ("nea-119-votes-pdf", "primary", ["scored_items"]),
        ],
        "HTML grade cells are primary and exactly corroborated by the official PDF. Scored-item citations and positions come from the separate publisher vote supplement.",
        year="2025",
        congress="119",
        session="1",
    )
    b.metric(
        "fixture:senate-grade",
        "119th Congressional Grade",
        "nea-119-html",
        "U.S. Senate Report Card / column heading",
        chamber_text="Senate",
        period_text="119th",
    )
    member = b.member(
        "fixture:senate:NES:pete-ricketts",
        "Pete Ricketts",
        "nea-119-html",
        "U.S. Senate Report Card / Pete Ricketts row",
        chamber_text="Senate",
        state="NES",
    )
    b.literal("scorecard_members", member, "state")
    b.rating(
        "fixture:senate-grade",
        member["publisher_member_key"],
        "F",
        "nea-119-html",
        "U.S. Senate Report Card / Pete Ricketts / 119th Congressional Grade",
    )
    path = "Physical page2 / On the Amendment No.2382 paragraph"
    item = b.row(
        "scorecard_items",
        "nea-119-votes-pdf",
        path,
        item_id="fixture:senate-amendment-2382",
        item_kind_text="amendment",
        title="On the Amendment No. 2382 to H.R.1",
        chamber_text="Senate",
        roll_number_text="1-\n358",
        bill_citation_text="H.R. 1",
        amendment_citation_text="S.Amdt. 2382 to S.Amdt. 2360 to H.R. 1",
        publisher_position_text="supported",
        position_basis="explicit publisher statement",
        position_source_path=path,
        references_json=encoded(
            [
                reference(
                    "source-outer-amendment", "S.Amdt. 2382", "amendment", path, amendment_citation_text="S.Amdt. 2382"
                ),
                reference(
                    "source-inner-amendment", "S.Amdt. 2360", "amendment", path, amendment_citation_text="S.Amdt. 2360"
                ),
                reference("source-bill", "H.R. 1", "bill", path, bill_citation_text="H.R. 1"),
            ]
        ),
    )
    b.literal("scorecard_items", item, "roll_number_text")
    return b.finish()


def humane_bundle():
    b = Bundle(
        "humane:2025:bounded-schema",
        "Three Senate members, 100+ and leadership exclusion ratings, and checked PDF result glyphs.",
        [
            "Full PDF captured; this fixture selects Senate observations only. House parsing must qualify before whole-edition replacement.",
            "Published result_text contains visually checked display glyphs. Raw extraction tokens remain outside table facts. A parser must qualify and version its font decoding for this PDF layout before reuse.",
            "Leadership credit supplies no standalone numeric adjustment ledger.",
        ],
    )
    b.edition_rows(
        "humane_world_action",
        "Humane World Action Fund",
        "2025",
        "Humane Scorecard",
        "2025",
        "Senate",
        [
            ("humane-2025-final", "primary", ["members", "metrics", "ratings", "items", "results", "methodology"]),
            ("humane-method", "corroboration", ["methodology"]),
        ],
        "Use the final PDF chart and embedded legend. Apply visually checked font decoding humane-final-2025-pdf-font-map-v1. Published result_text stores displayed glyphs; raw tokens and the checked mapping remain in extraction observations outside table facts. This decoding does not infer an action or numeric contribution.",
        year="2025",
        congress="119",
    )
    b.metric(
        "fixture:senate-score",
        "Score",
        "humane-2025-final",
        "Physical pages6–8 / Senate chart / Score",
        chamber_text="Senate",
        period_text="2025",
    )
    people = [
        (
            "fixture:senate:AZ:ruben-gallego",
            "Gallego, Ruben (D)",
            "Arizona",
            "D",
            "100+",
            "Physical page6 / Arizona / Gallego",
        ),
        (
            "fixture:senate:LA:john-kennedy",
            "Kennedy, John (R)",
            "Louisiana",
            "R",
            "100+",
            "Physical page7 / Louisiana / Kennedy",
        ),
        (
            "fixture:senate:NY:chuck-schumer",
            "Schumer, Chuck (D)",
            "New York",
            "D",
            "••",
            "Physical page7 / New York / Schumer",
        ),
    ]
    for key, name, state, party, value, path in people:
        b.member(key, name, "humane-2025-final", path, chamber_text="Senate", state=state, party=party)
        b.rating("fixture:senate-score", key, value, "humane-2025-final", path + " / Score")
    b.row(
        "scorecard_methodologies",
        "humane-2025-final",
        "Physical page7 / Key to Senate Chart / ••",
        methodology_id="fixture:leader-exclusion",
        chamber_text="Senate",
        disclosure_status="explicit",
        methodology_text="••",
        methodology_url=CAPTURES["humane-2025-final"]["resolved_url"],
    )
    for key, title in [("fixture:awa-enforcement", "AWA enforcement co-sponsor"), ("fixture:leader", "Leader")]:
        b.row(
            "scorecard_items",
            "humane-2025-final",
            "Physical page6 / Senate chart column heading",
            item_id=key,
            title=title,
            chamber_text="Senate",
        )
    for key, member, token, path in [
        ("fixture:awa-enforcement", people[0][0], "✓", people[0][-1] + " / AWA enforcement co-sponsor"),
        ("fixture:leader", people[1][0], "★", people[1][-1] + " / Leader"),
    ]:
        row = b.row(
            "scorecard_member_item_results",
            "humane-2025-final",
            path,
            result_id="fixture:pdf-cell",
            item_id=key,
            publisher_member_key=member,
            result_text=token,
        )
        b.literal("scorecard_member_item_results", row, "result_text")
    b.data["corroborating_observations"].extend(
        [
            {
                "capture_id": "humane-2025-final",
                "source_path": "Rendered physical page6 / Gallego / AWA enforcement",
                "raw_extraction": "ü",
                "checked_visible_glyph": "✓",
                "decoding_rule_version": "humane-final-2025-pdf-font-map-v1",
                "readback": "Rendered physical page6 shows the check in Gallego's AWA enforcement cell and the chart legend. The raw token is a font decoding artifact, not the published display character.",
            },
            {
                "capture_id": "humane-2025-final",
                "source_path": "Rendered physical page7 / Kennedy / Leader",
                "raw_extraction": "ê",
                "checked_visible_glyph": "★",
                "decoding_rule_version": "humane-final-2025-pdf-font-map-v1",
                "readback": "Rendered physical page7 shows the star in Kennedy's Leader cell and the chart legend. The raw token is a font decoding artifact, not the published display character.",
            },
            {
                "capture_id": "humane-2025-final",
                "source_path": "Physical page7 / Senate chart key",
                "literal": "••",
                "observation": "The publisher states top party leaders have no numerical scores. No conversion to zero or inferred numeric adjustment.",
            },
        ]
    )
    return b.finish()


def heritage_bundle():
    b = Bundle(
        "heritage_action:119:bounded-schema",
        "One explicit Senate cosponsorship item, one source-identified member, group membership and attached overall score.",
        [
            "A sponsor-group observation is not an official roll-call vote or a metric-specific numeric contribution.",
            "Historical full member-list editions remain refused because the route embeds current members.",
        ],
    )
    capture = "heritage119-cosponsor-s382-119"
    b.edition_rows(
        "heritage_action",
        "Heritage Action",
        "119",
        "Scorecard",
        "119",
        "Senate",
        [
            (capture, "primary", ["item", "member_identity", "member_result", "score"]),
            ("heritage-119-cosponsorships", "corroboration", ["item"]),
            ("heritage-119-members", "corroboration", ["member_identity", "score"]),
        ],
        "Exact item JSON is primary for this fixture. Its explicit sponsor group and attached overall score remain separate observations; public lists corroborate identity and scope.",
        congress="119",
    )
    b.metric(
        "fixture:overall-score",
        "score",
        capture,
        "data.sponsors.data[0].score",
        chamber_text="Senate",
        period_text="119",
    )
    member = "fixture:congId:B001243"
    b.member(
        member,
        "Marsha Blackburn",
        capture,
        "data.sponsors.data[0]",
        publisher_member_id="B001243",
        identifiers_json=encoded([{"scheme": "cong_id", "value": "B001243"}]),
        chamber_text="Senate",
        state="TN",
        district="00",
        party="R",
    )
    b.rating("fixture:overall-score", member, "98", capture, "data.sponsors.data[0].score", value_number="98")
    item = b.row(
        "scorecard_items",
        capture,
        "data",
        item_id="fixture:s382-119",
        publisher_item_id="s382-119",
        item_kind_text="cosponsorship",
        title="The Dismantle DEI Act",
        item_date_text="02/04/2025",
        congress_text="119",
        chamber_text="senate",
        bill_citation_text="S.382",
        publisher_position_text="yes",
        position_basis="explicit sponsor_position field",
        position_source_path="data.sponsor_position",
        references_json=encoded(
            [
                reference(
                    "source-bill",
                    "S.382",
                    "bill",
                    "data.number",
                    congress_text="119",
                    chamber_text="senate",
                    bill_citation_text="S.382",
                )
            ]
        ),
    )
    b.literal("scorecard_items", item, "publisher_position_text")
    result = b.row(
        "scorecard_member_item_results",
        capture,
        "data.sponsors.data[0]",
        result_id="fixture:sponsor-group-membership",
        item_id="fixture:s382-119",
        publisher_member_key=member,
        action_text="sponsors",
    )
    b.literal("scorecard_member_item_results", result, "action_text")
    return b.finish()


def composite_bundle():
    b = Bundle(
        "c4ip:2024:grades-and-component-challenges",
        "C4IP published grade and three dimension definitions, plus a separate Chamber methodology-only edition with explicit component weights.",
        [
            "Chamber is a source-attributed schema challenge, not a selected adapter or a member-data acquisition claim.",
            "No C4IP component member scores or Data Annex values are fabricated.",
            "No grade-to-number conversion, universal scoring formula or numeric member-adjustment ledger is asserted.",
        ],
    )
    b.edition_rows(
        "c4ip",
        "Council for Innovation Promotion",
        "2024",
        "Congressional Innovation Scorecard",
        "First Edition, March 2024",
        "Senate",
        [
            ("c4ip-2024-pdf", "primary", ["edition", "members", "grade", "dimensions", "methodology"]),
        ],
        "Published grade cells and dimension descriptions come from the same PDF. Methodology definitions create no invented member component values.",
        year="2024",
        periods=[
            period("116th Congress", "Physical page19", congress_text="116"),
            period("117th Congress", "Physical page19", congress_text="117"),
            period(
                "118th Congress (session 1, January-December 2023)",
                "Physical page19",
                congress_text="118",
                session_text="1",
                year_text="2023",
            ),
        ],
    )
    b.row(
        "scorecard_methodologies",
        "c4ip-2024-pdf",
        "Physical pages19–22 / dimensions and final grades",
        methodology_id="fixture:dimensions",
        methodology_text="Table 1: Scorecard Dimensions",
        disclosure_status="partial",
        methodology_url=CAPTURES["c4ip-2024-pdf"]["resolved_url"],
    )
    b.metric(
        "fixture:overall-grade",
        "Alphabetical Grade",
        "c4ip-2024-pdf",
        "Physical page28 / Table4 / column heading",
        chamber_text="Senate",
        methodology_id="fixture:dimensions",
    )
    # The numerical overall score is method-defined; this fixture supplies no member value for it.
    b.metric(
        "fixture:overall-numerical",
        "overall numerical score",
        "c4ip-2024-pdf",
        "Physical page22 / grading methodology",
        methodology_id="fixture:dimensions",
    )
    for key, name in [
        ("fixture:dimension-1", "Congressional voting record (current and historic)"),
        ("fixture:dimension-2", "Non-voting congressional and legislative activity (current and historic)"),
        ("fixture:dimension-3", "IP and innovation national leadership and advocacy"),
    ]:
        b.metric(key, name, "c4ip-2024-pdf", "Physical page19 / Table1", methodology_id="fixture:dimensions")
        b.row(
            "scorecard_metric_components",
            "c4ip-2024-pdf",
            "Physical pages19–22 / scorecard dimensions",
            parent_metric_id="fixture:overall-numerical",
            component_metric_id=key,
        )
    b.member(
        "fixture:senate:DE:christopher-coons",
        "Christopher Coons",
        "c4ip-2024-pdf",
        "Physical page28 / Table4 / Christopher Coons",
        chamber_text="Senate",
        state="DE",
        party="Democrat",
    )
    b.rating(
        "fixture:overall-grade",
        "fixture:senate:DE:christopher-coons",
        "A+",
        "c4ip-2024-pdf",
        "Physical page28 / Table4 / Christopher Coons / Alphabetical Grade",
    )
    # Independent publisher and edition: never attribute Chamber weights to C4IP.
    capture = "chamber-method-current"
    b.edition_rows(
        "us_chamber",
        "U.S. Chamber of Commerce",
        "116-methodology",
        "How They Voted",
        "116th Congress",
        "both",
        [
            (capture, "primary", ["methodology", "metric_definitions", "component_weights"]),
        ],
        "This independent Chamber edition contains only the captured methodology definitions and weights. No member rating rows are asserted.",
        congress="116",
    )
    b.row(
        "scorecard_methodologies",
        capture,
        "Annual Score / component sections",
        methodology_id="fixture:annual-composite",
        methodology_text="The Annual score has three components: Legislative (80%), Leadership (10%), and Bipartisanship (10%).",
        disclosure_status="explicit",
        methodology_url=CAPTURES[capture]["resolved_url"],
    )
    b.metric(
        "fixture:annual", "Annual Score", capture, "Annual Score heading", methodology_id="fixture:annual-composite"
    )
    for key, name, weight in [
        ("fixture:legislative", "Legislative", "80%"),
        ("fixture:leadership", "Leadership", "10%"),
        ("fixture:bipartisanship", "Bipartisanship", "10%"),
    ]:
        b.metric(key, name, capture, name + " section", methodology_id="fixture:annual-composite")
        component = b.row(
            "scorecard_metric_components",
            capture,
            name + " section heading",
            parent_metric_id="fixture:annual",
            component_metric_id=key,
            weight_text=weight,
        )
        b.literal("scorecard_metric_components", component, "weight_text")
    return b.finish()


def build():
    return {
        "schema_version": SCHEMA["version"],
        "fixture_only": True,
        "meaning": "Every complete snapshot is closure of its stated bounded fixture, never a production or full-edition completeness claim.",
        "numeric_adjustment_ledger": "Deferred: no sampled standalone per-member numeric ledger was qualified.",
        "bundles": [afl_bundle(), lcv_bundle(), nea_bundle(), humane_bundle(), heritage_bundle(), composite_bundle()],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    serialized = json.dumps(build(), ensure_ascii=False, indent=2) + "\n"
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != serialized:
            raise SystemExit("sample_bundles.json differs; regenerate with build_sample_bundles.py")
        print("Sample bundle output matches the checked source-grounded builder.")
    else:
        OUTPUT.write_text(serialized)
        print("Wrote " + str(OUTPUT))


if __name__ == "__main__":
    main()
