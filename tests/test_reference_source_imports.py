"""Reference source adapters remain optional until their readers are used."""

import json
import subprocess
import sys

import pytest


IMPORT_SCRIPT = """
import importlib.abc
import json
import sys

class NoSourceReaders(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "spicy_docs" or fullname.startswith("spicy_docs."):
            raise ModuleNotFoundError("dependency deliberately absent", name=sys.argv[1])

sys.meta_path.insert(0, NoSourceReaders())
import spicy_regs.cli
import spicy_regs.mcp_server
import spicy_regs.transforms
import spicy_regs.pipelines.rollups.courtlistener
import spicy_regs.pipelines.rollups.crs_reports
import spicy_regs.pipelines.rollups.gao_reports
import spicy_regs.pipelines.rollups.fcc_filings
import spicy_regs.pipelines.rollups.fcc_proceedings
import spicy_regs.pipelines.rollups.usaspending_recipients
from spicy_regs.sources.courtlistener import CourtListenerReader, CourtListenerOpinionSearchReader
from spicy_regs.sources.crs_reports import CrsReportsReader
from spicy_regs.sources.gao_reports import GaoReportsReader
from spicy_regs.sources.fcc_ecfs import FccEcfsFilingsReader, FccEcfsProceedingsReader
from spicy_regs.sources.usaspending import UsaSpendingRecipientsReader

readers = (
    CourtListenerReader(),
    CourtListenerOpinionSearchReader(),
    CrsReportsReader(api_key="fixture-unused"),
    GaoReportsReader(),
    FccEcfsFilingsReader(api_key="fixture-unused"),
    FccEcfsProceedingsReader(api_key="fixture-unused"),
    UsaSpendingRecipientsReader(),
)
assert not any(name == "spicy_docs" or name.startswith("spicy_docs.") for name in sys.modules)
results = []
for reader in readers:
    try:
        next(reader.iter_records())
    except RuntimeError as error:
        assert sys.argv[1] == "spicy_docs"
        assert "spicy-regs[source-readers]" in str(error)
        assert "uv sync --frozen" in str(error)
        results.append({"reader": type(reader).__name__, "error": str(error)})
    except ModuleNotFoundError as error:
        assert sys.argv[1] != "spicy_docs" and error.name == sys.argv[1]
        results.append({"reader": type(reader).__name__, "missing_dependency": error.name})
    else:
        raise AssertionError("missing reader dependency became an empty success")
assert not any(name == "spicy_docs" or name.startswith("spicy_docs.") for name in sys.modules)
print(json.dumps(results))
"""


@pytest.mark.parametrize("missing_name", ["spicy_docs", "broken_source_dependency"])
def test_base_imports_and_construction_are_optional_but_source_use_refuses(missing_name):
    result = subprocess.run([sys.executable, "-c", IMPORT_SCRIPT, missing_name], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert len(json.loads(result.stdout)) == 7


def test_inert_source_defaults_agree_with_installed_provider():
    from spicy_docs.sources.gao.rss import GAO_REPORTS_FEED_URL
    from spicy_docs.sources.usaspending import MAX_LIMIT
    from spicy_regs.sources.gao_reports import RSS_URL
    from spicy_regs.sources.usaspending import PER_PAGE

    assert RSS_URL == GAO_REPORTS_FEED_URL
    assert PER_PAGE == MAX_LIMIT
