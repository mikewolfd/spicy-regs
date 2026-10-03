"""Transform: build ``agency_lifecycle_stats.parquet``, the cumulative incidence of a final per agency and routine family.

Completed-only percentiles understate time to final, because the slow rules are the ones still
open: on snapshot_7eafd657 the median was 156 days completed-only against 265 once open
proposals were counted (decision 54). A withdrawn proposal never becomes final, so withdrawal
is a competing outcome, not censoring (decision 54a): the estimate is the
Aalen-Johansen cumulative incidence of a final. The estimator is small and standard, so it
lives here, tested against a hand-computed example and checked against R's survival package,
rather than making SciPy (a development-only dependency here) a runtime one.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.common import ATTESTATION_COLUMNS, RunContext, iter_parquet_rows
from spicy_regs.transforms.build_lifecycles import LIFECYCLES_OUTPUT, ROUTINE_FAMILIES

OUTPUT = "agency_lifecycle_stats.parquet"
# v2 (one bump over published v1): code unchanged; its cells move with lifecycles v2 (decisions 54d and 55a).
# v3 (one bump over published v2): code unchanged; FNS's cells move to FNA with lifecycles v3 (proceedings v12).
ACTOR_ID = "spicy-regs:agency-lifecycle-stats:v3"

#: A cell with fewer rules than this keeps its row, flagged, with no estimates (decision 55).
MIN_RULES = 30
CONFIDENCE = 0.95
#: The quantiles of time to final reported, by column prefix: the quartiles and the median.
QUANTILES = (("q1", 0.25), ("median", 0.5), ("q3", 0.75))
STRATA = ("all", "routine", "non_routine", *(family for family, *_ in ROUTINE_FAMILIES))
#: A lifecycle's ``outcome``: a final, the competing withdrawal, or right-censored at the censor date.
OUTCOMES = ("final", "withdrawn", "censored")
#: A standard error under this is rounding in the influence sums, not a variance.
_NOISE = 1e-9

ESTIMATOR = (
    "Aalen-Johansen cumulative incidence F(t) of a final, days from proposal; a withdrawal is a competing "
    "event (a withdrawn proposal stays in the denominator and never becomes final) and an open proposal is "
    "right-censored at the censor date. Companions, upload pairs and lifecycles with no proposal are not in "
    "it. A quantile p is the first day F(t) >= p, NULL where F never reaches p: where F equals p over a flat "
    "stretch it is that stretch's first day, not the midpoint to the next rise that R's quantile.survfit "
    "takes for a single-event curve (R computes no quantiles for multi-state curves). Its 95% interval runs "
    "from the first day the pointwise band's upper edge reaches p to the first day its lower edge does, NULL "
    "where an edge never does. The band is F^exp(+-1.96 se / (F |log F|)) (log-log), se the "
    "infinitesimal-jackknife standard error; F, se and the band are what R survival's multi-state "
    "survfit(Surv(days, outcome) ~ 1, conf.type = 'log-log') reports as pstate, std.err, lower and upper."
)

SCHEMA = pa.schema(
    [
        ("agency_code", pa.string()),
        ("stratum", pa.string()),
        ("rules", pa.int32()),
        ("finals", pa.int32()),
        ("withdrawals", pa.int32()),
        ("censored", pa.int32()),
        ("suppressed", pa.bool_()),
        *(
            (f"{name}{suffix}", pa.int32())
            for name, _ in QUANTILES
            for suffix in ("_days", "_lower_days", "_upper_days")
        ),
        ("censor_date", pa.date32()),
        *((column, pa.string()) for column in ATTESTATION_COLUMNS),
    ],
    metadata={b"estimator": ESTIMATOR.encode()},
)


@dataclass(frozen=True)
class IncidenceStep:
    """The cumulative incidence of a final just after one day on which finals occurred."""

    day: int
    at_risk: int
    finals: int
    incidence: float
    std_err: float
    lower: float
    upper: float


def aalen_johansen(
    durations: Sequence[int], outcomes: Sequence[str], confidence: float = CONFIDENCE
) -> list[IncidenceStep]:
    """The Aalen-Johansen cumulative incidence of a final, withdrawal competing, with its log-log band.

    On each day with ``d_f`` finals and ``d_w`` withdrawals among ``n`` rules still at risk (a rule
    censored on a day is at risk on it), ``F`` grows by ``P · d_f/n`` and the probability ``P`` of
    being still pending shrinks by ``1 - (d_f + d_w)/n``. The standard error is the infinitesimal
    jackknife's, ``√Σᵢ (∂F/∂wᵢ)²``, as R's multi-state survfit computes it. Every rule still at
    risk has the same influence, and a rule that has left evolves by one linear map a day, so the
    sum is kept as second moments: O(distinct days), not O(days × rules). The band is log-log on
    ``F``; where ``F`` is 1 it is undefined (NaN).
    """
    z = NormalDist().inv_cdf(0.5 + confidence / 2)
    by_day: dict[int, Counter[str]] = defaultdict(Counter)
    for day, outcome in zip(durations, outcomes, strict=True):
        if outcome not in OUTCOMES:
            raise ValueError(f"unknown lifecycle outcome {outcome!r}")
        by_day[day][outcome] += 1
    at_risk, pending, incidence = len(durations), 1.0, 0.0
    # The influence (on pending, on incidence) every rule still at risk shares, and the second
    # moments of the influences of the rules that have left.
    shared_a = shared_b = 0.0
    left_aa = left_ab = left_bb = 0.0
    steps = []
    for day in sorted(by_day):
        finals, withdrawals, censored = (by_day[day][outcome] for outcome in OUTCOMES)
        n = at_risk
        if finals or withdrawals:
            h_final, h_any = finals / n, (finals + withdrawals) / n
            # Rules already gone: a' = a(1 - h), b' = b + a·h_final.
            left_aa, left_ab, left_bb = (
                (1 - h_any) ** 2 * left_aa,
                (1 - h_any) * (left_ab + h_final * left_aa),
                left_bb + 2 * h_final * left_ab + h_final**2 * left_aa,
            )
            common_a, common_b = shared_a * (1 - h_any), shared_b + shared_a * h_final
            stays = (common_a + pending * h_any / n, common_b - pending * h_final / n)
            finalized = (common_a - pending * (1 - h_any) / n, common_b + pending * (1 - h_final) / n)
            withdrawn = (common_a - pending * (1 - h_any) / n, common_b - pending * h_final / n)
            incidence += pending * h_final
            pending *= 1 - h_any
            for count, (a, b) in ((finals, finalized), (withdrawals, withdrawn), (censored, stays)):
                left_aa, left_ab, left_bb = left_aa + count * a * a, left_ab + count * a * b, left_bb + count * b * b
            shared_a, shared_b = stays
        else:
            left_aa += censored * shared_a * shared_a
            left_ab += censored * shared_a * shared_b
            left_bb += censored * shared_b * shared_b
        at_risk -= finals + withdrawals + censored
        if finals:
            variance = left_bb + at_risk * shared_b * shared_b
            # Where every rule has left through a final the variance is 0, which these sums reach only
            # to ~1e-19; R reports exactly 0 there, and its band is [F, F]. No real standard error at
            # these sizes is under ~1/n, far above the cut.
            std_err = math.sqrt(variance) if variance > _NOISE**2 else 0.0
            if 0 < incidence < 1:
                width = min(z * std_err / (incidence * abs(math.log(incidence))), 700.0)
                lower, upper = incidence ** math.exp(width), incidence ** math.exp(-width)
            else:
                lower = upper = math.nan
            steps.append(IncidenceStep(day, n, finals, incidence, std_err, lower, upper))
    return steps


def quantile(steps: Sequence[IncidenceStep], p: float) -> tuple[int | None, int | None, int | None]:
    """The first day the cumulative incidence of a final reaches ``p``, with its interval; ``None`` where it never does.

    The interval runs from the first day the band's upper edge reaches ``p`` to the first day its
    lower edge does (Brookmeyer and Crowley's inversion). An undefined edge (NaN) reaches nothing.
    """
    target = p - 1e-9  # a sum of exact ratios lands a hair either side of p

    def first(edge: str) -> int | None:
        return next((step.day for step in steps if getattr(step, edge) >= target), None)

    return first("incidence"), first("upper"), first("lower")


def build_agency_lifecycle_stats(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Estimate the cumulative incidence of a final per agency (and for all agencies, ``agency_code`` NULL).

    A cell holds the lifecycles with an ``outcome`` (``finalized``, ``withdrawn`` and
    ``open``): ``all`` of them, ``routine`` or ``non_routine``, and each routine family.
    """
    source = output_dir / LIFECYCLES_OUTPUT
    if not source.exists():
        raise FileNotFoundError(f"agency lifecycle stats input missing from {output_dir}: {LIFECYCLES_OUTPUT}")
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="agency-lifecycle-stats")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    cells: dict[tuple[str | None, str], tuple[list[int], list[str]]] = defaultdict(lambda: ([], []))
    censor_dates = set()
    for row in iter_parquet_rows(
        source, columns=("agency_code", "routine_family", "outcome", "duration_days", "censor_date")
    ):
        censor_dates.add(row["censor_date"])
        if row["outcome"] is None:
            continue
        family = row["routine_family"]
        strata = ("all", "routine", family) if family else ("all", "non_routine")
        for agency in {row["agency_code"], None}:
            for stratum in strata:
                durations, outcomes = cells[(agency, stratum)]
                durations.append(row["duration_days"])
                outcomes.append(row["outcome"])
    if len(censor_dates) > 1:
        raise RuntimeError(f"lifecycles carry {len(censor_dates)} censor dates, not one: {sorted(censor_dates)}")
    censor_date = next(iter(censor_dates), None)

    rows = []
    for (agency, stratum), (durations, outcomes) in sorted(
        cells.items(), key=lambda item: (item[0][0] is not None, item[0][0] or "", STRATA.index(item[0][1]))
    ):
        suppressed = len(durations) < MIN_RULES
        counts = Counter(outcomes)
        estimates: dict[str, int | None] = {}
        steps = [] if suppressed else aalen_johansen(durations, outcomes)
        for name, p in QUANTILES:
            days, lower, upper = (None, None, None) if suppressed else quantile(steps, p)
            estimates |= {f"{name}_days": days, f"{name}_lower_days": lower, f"{name}_upper_days": upper}
        rows.append(
            {
                "agency_code": agency,
                "stratum": stratum,
                "rules": len(durations),
                "finals": counts["final"],
                "withdrawals": counts["withdrawn"],
                "censored": counts["censored"],
                "suppressed": suppressed,
                **estimates,
                "censor_date": censor_date,
                **provenance,
            }
        )
    out_file = output_dir / OUTPUT
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), out_file, compression="zstd")
    logger.info(
        "Agency lifecycle stats: {:,} cells, {:,} suppressed under {} rules, {:,} medians never reached",
        len(rows),
        sum(row["suppressed"] for row in rows),
        MIN_RULES,
        sum(not row["suppressed"] and row["median_days"] is None for row in rows),
    )
    return out_file
