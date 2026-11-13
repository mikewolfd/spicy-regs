"""Latest complete observed summary and explicit differences between held snapshots."""

from .sql_views import SQLView

FOREIGN_ASSISTANCE_VIEWS = (
    SQLView(
        name="foreign_assistance_latest",
        required={"foreign_assistance_observations": ("snapshot_id",),
                  "foreign_assistance_snapshots": ("snapshot_id", "observed_at", "source_sha256", "dictionary_sha256")},
        query=lambda _: """
            SELECT o.*, s.observed_at, s.source_sha256, s.dictionary_sha256
            FROM foreign_assistance_observations o JOIN foreign_assistance_snapshots s USING (snapshot_id)
            WHERE s.snapshot_id = (SELECT snapshot_id FROM foreign_assistance_snapshots
                                   ORDER BY observed_at DESC, snapshot_id DESC LIMIT 1)
        """,
        meaning="Rows of the latest complete observed summary file. Reporting may be partial or revised; "
                "constant-dollar price base is unspecified. An absent recipient is unobserved, never zero.",
        identity_columns=("recipient_code", "recipient_name", "fiscal_period", "measure_id", "dollar_basis"),
        rule_version="foreign-assistance-observed-v1", role="child_query", category="query_data",
    ),
    SQLView(
        name="foreign_assistance_revisions",
        required={"foreign_assistance_observations": (
            "snapshot_id", "recipient_code", "recipient_name", "fiscal_period", "measure_id", "dollar_basis",
            "amount", "amount_raw", "amount_status", "source_locator_json",
        ), "foreign_assistance_snapshots": ("snapshot_id", "observed_at")},
        query=lambda _: """
            WITH ordered AS (
                SELECT snapshot_id, observed_at,
                       lag(snapshot_id) OVER (ORDER BY observed_at, snapshot_id) previous_snapshot_id
                FROM foreign_assistance_snapshots
            ), keys AS (
                SELECT DISTINCT recipient_code, recipient_name, fiscal_period, measure_id, dollar_basis
                FROM foreign_assistance_observations
            )
            SELECT s.snapshot_id, s.previous_snapshot_id, s.observed_at, k.*,
                   p.amount_raw previous_amount_raw, n.amount_raw current_amount_raw,
                   p.amount_status previous_amount_status, n.amount_status current_amount_status,
                   p.source_locator_json previous_source_locator_json,
                   n.source_locator_json current_source_locator_json,
                   CASE WHEN p.snapshot_id IS NULL THEN 'newly_observed'
                        WHEN n.snapshot_id IS NULL THEN 'now_unobserved'
                        WHEN p.amount_raw IS DISTINCT FROM n.amount_raw THEN 'changed'
                        ELSE 'unchanged' END observation_change,
                   CASE WHEN k.dollar_basis = 'current' THEN n.amount - p.amount END current_usd_change
            FROM ordered s CROSS JOIN keys k
            LEFT JOIN foreign_assistance_observations p ON p.snapshot_id = s.previous_snapshot_id
                 AND (p.recipient_code, p.recipient_name, p.fiscal_period, p.measure_id, p.dollar_basis)
                     = (k.recipient_code, k.recipient_name, k.fiscal_period, k.measure_id, k.dollar_basis)
            LEFT JOIN foreign_assistance_observations n ON n.snapshot_id = s.snapshot_id
                 AND (n.recipient_code, n.recipient_name, n.fiscal_period, n.measure_id, n.dollar_basis)
                     = (k.recipient_code, k.recipient_name, k.fiscal_period, k.measure_id, k.dollar_basis)
            WHERE s.previous_snapshot_id IS NOT NULL AND (p.snapshot_id IS NOT NULL OR n.snapshot_id IS NOT NULL)
        """,
        meaning="Adjacent held summary observations; now_unobserved does not establish deletion or zero. "
                "Constant-dollar values remain visible but receive no cross-version delta because their price base is unknown.",
        identity_columns=("snapshot_id", "recipient_code", "recipient_name", "fiscal_period", "measure_id", "dollar_basis"),
        rule_version="foreign-assistance-observed-v1", role="child_query", category="query_data",
        column_descriptions={
            "previous_snapshot_id": "Identifier of the immediately preceding held snapshot ordered by observed_at, "
                                    "then snapshot_id; this is an observed-file comparison, not a publisher revision order.",
            "observation_change": "newly_observed, now_unobserved, changed or unchanged between adjacent held files.",
            "current_usd_change": "Current-dollar difference only when both amounts are numeric; NULL otherwise.",
        },
    ),
)
