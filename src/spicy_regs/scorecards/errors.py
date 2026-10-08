"""Scorecard refresh outcomes shared by acquisition and the source build."""


class NoScorecardsDue(RuntimeError):
    """Successful no-op; do not create a generation."""


class ScorecardRefreshError(RuntimeError):
    """No accepted scopes or an integrity failure; preserve the prior generation."""
