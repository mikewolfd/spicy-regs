"""Pool repeated passes of an offset walk until the publisher's count is reached.

An offset walk over a list whose order shifts while it is read (ties in the
sort key, rows updated mid-walk) can repeat one record and skip another while
its row count still equals the declared count, so no single pass proves it is
complete. Measured: on 2026-09-23 the 119th Congress amendments walk returned
its declared 7,066 rows but 7,013 distinct amendments, and the 2026-09-22
scheduled CRS report and ECFS filing runs each refused on a repeated identity
that a replay the next day did not see (fork output ledger, 2026-09-23).

:func:`pool_passes` treats each pass as evidence: records are pooled by
identity over passes until the distinct count reaches what the publisher
declared, and a query still short after every pass refuses. A clean first pass
costs nothing extra.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Mapping

from loguru import logger


class IncompleteWalkError(RuntimeError):
    """Every pass ended short of the declared count."""


def pool_passes[R: Mapping](
    passes: Iterable[tuple[Iterable[R], int | None]],
    *,
    key: Callable[[R], Hashable],
    newer: Callable[[R, R], bool] = lambda held, record: True,
    label: str,
) -> list[R]:
    """Pool ``(records, declared)`` passes by ``key`` until the distinct count reaches ``declared``.

    ``passes`` is consumed lazily (a generator walking each pass when asked), so a later pass
    is requested only when the pool is still short. A record seen again replaces the held one
    when ``newer(held, record)``. A pass declaring no count ends the walk.
    """
    pool: dict[Hashable, R] = {}
    declared: int | None = None
    count = 0
    for count, (records, declared) in enumerate(passes, 1):
        for record in records:
            identity = key(record)
            held = pool.get(identity)
            if held is None or newer(held, record):
                pool[identity] = record
        if declared is None or len(pool) >= declared:
            if count > 1:
                logger.info("{}: {} passes pooled {:,} records", label, count, len(pool))
            return list(pool.values())
    if count == 0:
        raise IncompleteWalkError(f"{label}: no pass was walked")
    raise IncompleteWalkError(f"{label}: pooled {len(pool):,} of {declared:,} declared records after {count} passes")
