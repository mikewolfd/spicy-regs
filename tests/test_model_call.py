"""Where this host's Gemini key comes from.

The adapter these tests used to cover — the request body, the schema on the
request, the candidate envelope, the refusal that does not echo model prose,
the token counts — moved to ``spicy_docs.interpretation.gemini_call`` in
0.22.0 and is proved in that repository's
``tests/test_interpretation_gemini_call.py``, beside the code that decides it.
Re-asserting it here would be a second copy of the same claim that can only
drift, so it went with the code. What is owned here is which environment
variable this application reads.
"""

from __future__ import annotations

from spicy_regs.transforms.model_call import resolve_gemini_key


def test_no_key_set_resolves_to_none(monkeypatch):
    """A keyless run must skip the model tables, not fail and not guess."""
    for name in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    assert resolve_gemini_key() is None


def test_the_first_set_key_wins(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "first")
    monkeypatch.setenv("GOOGLE_API_KEY", "second")
    assert resolve_gemini_key() == "first"


def test_the_second_variable_is_read_when_the_first_is_unset(monkeypatch):
    """Both names are supported on purpose; only the first being read is a silent skip."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "second")
    assert resolve_gemini_key() == "second"


def test_an_empty_variable_is_not_a_key(monkeypatch):
    """An exported-but-empty variable must fall through, not wire a keyless client."""
    monkeypatch.setenv("GEMINI_API_KEY", "")
    monkeypatch.setenv("GOOGLE_API_KEY", "second")
    assert resolve_gemini_key() == "second"
