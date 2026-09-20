from spicy_regs.pipelines.base import Pipeline

__all__ = ["Pipeline", "RegulationsPipeline"]


def __getattr__(name):
    if name == "RegulationsPipeline":
        from spicy_regs.pipelines.regulations import RegulationsPipeline

        return RegulationsPipeline
    raise AttributeError(name)
