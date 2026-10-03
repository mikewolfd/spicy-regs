"""One rule for reading a credential from the first of several environment variables (DRY work list F34)."""

from __future__ import annotations

from importlib import import_module

import pytest

#: Every source's key resolver and the variables it reads, in its own order.
RESOLVERS = [
    ("spicy_regs.sources.congress_bills", "_resolve_api_key", "API_KEY_ENV_VARS"),
    ("spicy_regs.sources.cfr_sections", "_resolve_api_key", "API_KEY_ENV_VARS"),
    ("spicy_regs.sources.crs_reports", "_resolve_api_key", "API_KEY_ENV_VARS"),
    ("spicy_regs.transforms.build_fec_committees", "_resolve_api_key", "API_KEY_ENV_VARS"),
    ("spicy_regs.transforms.build_fcc_ecfs", "_resolve_api_key", "API_KEY_ENV_VARS"),
    ("spicy_regs.transforms.build_sam_entities", "_resolve_sam_api_key", "SAM_API_KEY_ENV_VARS"),
]


@pytest.mark.parametrize(("module_name", "resolver", "names"), RESOLVERS)
def test_every_key_resolver_skips_a_blank_value_and_strips_the_key_it_returns(monkeypatch, module_name, resolver,
                                                                             names):
    """A key read with a stray newline is sent and scrubbed from evidence as the key, whichever source reads it.

    ``congress_bills`` returned a whitespace-only value and kept a trailing
    newline, while ``cfr_sections`` and ``crs_reports`` stripped both: three
    copies of one loop with two rules.
    """
    module = import_module(module_name)
    first, second, *_rest = getattr(module, names)
    for name in getattr(module, names):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(first, "   ")
    monkeypatch.setenv(second, " the-key\n")
    assert getattr(module, resolver)() == "the-key"


def test_first_env_reads_the_first_variable_with_a_value_and_none_when_none_has_one(monkeypatch):
    from spicy_regs.env_values import first_env

    names = ("SPICY_TEST_KEY_A", "SPICY_TEST_KEY_B", "SPICY_TEST_KEY_C")
    for name in names:
        monkeypatch.delenv(name, raising=False)
    assert first_env(names) is None
    monkeypatch.setenv("SPICY_TEST_KEY_C", "c")
    monkeypatch.setenv("SPICY_TEST_KEY_B", "\t")
    assert first_env(names) == "c"
    monkeypatch.setenv("SPICY_TEST_KEY_A", "a")
    assert first_env(names) == "a"
