"""RefSpec 1e110da2 test-only oracle; no deliberate verdict divergences."""
import re

import pytest

from spicy_regs.ontology import ccn


class NppesIdentifierError(ValueError):
    pass


_CCN_SHAPE = re.compile('^\\d{2}[0-9A-Z]{4}$|^\\d{2}[0-9A-Z]{8}$')


def oracle(value: str) -> str:
    """Validate one CMS Certification Number's six- or ten-character shape."""

    if not isinstance(value, str) or not value.strip():
        raise NppesIdentifierError("CCN must not be empty")
    text = value.strip()
    if not _CCN_SHAPE.fullmatch(text):
        raise NppesIdentifierError(
            "CCN must be a 2-digit state code followed by a 4- or 8-character "
            "uppercase alphanumeric facility-type/sequence suffix"
        )
    return text


def verdict(function, value):
    try:
        return ("accepted", function(value))
    except ValueError as error:
        return (type(error).__name__, str(error))


@pytest.mark.parametrize("value", [None, 12, b"010001", [], {}, "", " ", "010001", "01ABCD", "01ABCDEFGH", "01abcD", "01000", "0100000", "01000000000", " 010001\n", "\u00a0010001\u00a0", "٠١0001", "０１0001", "01٠001", "01ＡBCD", "01AB\nCD", "01ABCD\nX"])
def test_verdict_and_normalization_equal_oracle(value):
    assert verdict(ccn.validate_ccn, value) == verdict(oracle, value)


def test_definition_tamper_refuses(monkeypatch):
    monkeypatch.setattr(ccn, "DEFINITION_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="definition hash mismatch"):
        ccn.validate_ccn("010001")
