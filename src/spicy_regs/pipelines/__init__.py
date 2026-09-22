"""Pipeline entry points: the base Pipeline contract and the lazily exported ETL."""

from spicy_regs.pipelines.base import Pipeline

__all__ = ["Pipeline", "RegulationsPipeline"]


def __getattr__(name):
    """Import RegulationsPipeline on first access rather than at package import."""
    if name == "RegulationsPipeline":
        from spicy_regs.pipelines.regulations import RegulationsPipeline

        return RegulationsPipeline
    raise AttributeError(name)
