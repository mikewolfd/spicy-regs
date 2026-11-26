"""Congress sessions and party control pipeline and transform tests."""

from __future__ import annotations

from datetime import date

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.congress.party_divisions import (
    HOUSE_PARTY_DIVISIONS_URL,
    HOUSE_SESSION_DATES_URL,
    SENATE_PARTY_DIVISION_URL,
)
from spicy_regs import local_publication
from spicy_regs.pipelines import congress_sessions as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_congress_sessions import PARTY_CONTROL, SCHEMAS, SESSIONS

HOUSE_SESSIONS_HTML = """<!doctype html><html><body><table>
<tr><th>Congress</th><th>Session</th><th>Beginning</th><th>Adjournment</th><th>Cal</th><th>Leg</th><th>Notes</th></tr>
<tr><td>57th (1901–1903)</td><td>1</td><td>Dec 2, 1901</td><td>Jul 1, 1902</td><td>212</td><td>142</td><td></td></tr>
<tr><td>68th (1923–1925)</td><td>2</td><td>Dec 1, 1924</td><td>Mar 4, 1925</td><td>94</td><td>77</td><td></td></tr>
<tr><td>71st (1929–1931)</td><td>1</td><td>Apr 15, 1929</td><td>Nov 22, 1929</td><td>222</td><td>117</td><td></td></tr>
<tr><td>119th (2025–2027)</td><td>1</td><td>Jan 3, 2025</td><td></td><td></td><td></td><td></td></tr>
</table></body></html>""".encode("utf-8")

HOUSE_DIVISIONS_HTML = """<!doctype html><html><body><table>
<tr><th>Congress (Years)</th><th>Seats</th><th>Democrats</th><th>Republicans</th><th>Other</th><th>Delegates</th></tr>
<tr><td>57th (1901–1903)</td><td>357</td><td>151</td><td>198</td><td>8</td><td>2</td></tr>
<tr><td>68th (1923–1925)</td><td>435</td><td>207</td><td>225</td><td>3</td><td>2</td></tr>
<tr><td>71st (1929–1931)</td><td>435</td><td>167</td><td>267</td><td>1</td><td>2</td></tr>
<tr><td>65th (1917–1919)<sup>1</sup></td><td>435</td><td>214</td><td>215</td><td>6</td><td>2</td></tr>
<tr><td>119th (2025–2027)</td><td>435</td><td>215</td><td>220</td><td>0</td><td>6</td></tr>
</table>
<p><sup>1</sup>Plurality footnote test.</p>
</body></html>""".encode("utf-8")

SENATE_DIVISION_HTML = """<!doctype html><html><body><table>
<tr class="head"><td><date>57th Congress (3/4/1901–3/3/1903)</date></td><td><majority>Republicans</majority></td><td><minority>Democrats</minority></td></tr>
<tr><th><header>1st session (12/1/1901–7/1/1902)</header></th><th><header>Republicans</header></th><th><header>Democrats</header></th></tr>
<tr><td><date>--</date></td><td><party1>56</party1></td><td><party2>31</party2></td><td><other_party>3</other_party></td><td><vacancies>0</vacancies></td><td><total>90</total></td></tr>

<tr class="head"><td><date>65th Congress (3/4/1917–3/3/1919)</date></td><td><majority>Democrats</majority></td><td><minority>Republicans</minority></td></tr>
<tr><th><header>1st session (4/2/1917–10/6/1917)</header></th><th><header>Democrats</header></th><th><header>Republicans</header></th></tr>
<tr><td><date>--</date></td><td><party1>53</party1></td><td><party2>42</party2></td><td><other_party>1</other_party></td><td><vacancies>0</vacancies></td><td><total>96</total></td></tr>

<tr class="head"><td><date>68th Congress (3/4/1923–3/3/1925)</date></td><td><majority>Republicans</majority></td><td><minority>Democrats</minority></td></tr>
<tr><th><header>2nd session (12/1/1922–3/3/1925)</header></th><th><header>Republicans</header></th><th><header>Democrats</header></th></tr>
<tr><td><date>--</date></td><td><party1>51</party1></td><td><party2>43</party2></td><td><other_party>2</other_party></td><td><vacancies>0</vacancies></td><td><total>96</total></td></tr>

<tr class="head"><td><date>71st Congress (3/4/1929–3/3/1931)</date></td><td><majority>Republicans</majority></td><td><minority>Democrats</minority></td></tr>
<tr><th><header>1st session (3/4/1929–3/5/1929)</header></th><th><header>Republicans</header></th><th><header>Democrats</header></th></tr>
<tr><td><date>--</date></td><td><party1>56</party1></td><td><party2>39</party2></td><td><other_party>1</other_party></td><td><vacancies>0</vacancies></td><td><total>96</total></td></tr>
<tr><th><header>1st (E) session (4/15/1929–11/22/1929)</header></th><th><header>Republicans</header></th><th><header>Democrats</header></th></tr>
<tr><td><date>--</date></td><td><party1>56</party1></td><td><party2>39</party2></td><td><other_party>1</other_party></td><td><vacancies>0</vacancies></td><td><total>96</total></td></tr>

<tr class="head"><td><date>119th Congress (1/3/2025–1/3/2027)</date></td><td><majority>Republicans</majority></td><td><minority>Democrats</minority></td></tr>
<tr><th><header>1st session (1/3/2025–)</header></th><th><header>Republicans</header></th><th><header>Democrats</header></th></tr>
<tr><td><date>--</date></td><td><party1>53</party1></td><td><party2>45</party2></td><td><other_party>2</other_party></td><td><vacancies></vacancies></td><td><total>100</total></td></tr>
</table></body></html>""".encode("utf-8")


def _transport():
    pages = {
        HOUSE_SESSION_DATES_URL: HOUSE_SESSIONS_HTML,
        HOUSE_PARTY_DIVISIONS_URL: HOUSE_DIVISIONS_HTML,
        SENATE_PARTY_DIVISION_URL: SENATE_DIVISION_HTML,
    }
    return httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(pages[str(request.url)]),
            headers={"content-type": "text/html; charset=utf-8"},
        )
    )


def test_congress_sessions_acquire_build_and_local_dry_run(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    selection = pipeline.acquire(
        tmp_path / "acq",
        archive_root=raw,
        transport=_transport(),
        min_request_interval_seconds=0.0,
    )
    assert set(selection["sources"]) == {"house_session_dates", "house_party_divisions", "senate_party_division"}

    out = tmp_path / "build"
    artifact, evidence = pipeline.build(selection, out, archive_root=raw)
    assert artifact.pin.artifact_digest and (evidence / "journal.jsonl").exists()

    for name, schema in SCHEMAS.items():
        actual = pq.read_schema(out / "generation" / f"{name}.parquet")
        assert actual.equals(schema)

    sessions = pq.read_table(out / "generation" / f"{SESSIONS}.parquet").to_pylist()
    by_cs = {(row["congress"], row["session"]): row for row in sessions}
    assert by_cs[(57, "1")]["disagreement_kind"] == "senate-off-by-one-day"
    assert by_cs[(57, "1")]["begins"] == date(1901, 12, 2)
    assert by_cs[(57, "1")]["senate_begins"] == date(1901, 12, 1)
    assert by_cs[(68, "2")]["disagreement_kind"] == "senate-mistyped-year"
    assert by_cs[(68, "2")]["correction_reason"]
    assert by_cs[(71, "1")]["senate_session"] == "1 (e)"
    assert by_cs[(71, "1 (senate-special)")]["disagreement_kind"] == "chamber-session-labeling"
    assert by_cs[(119, "1")]["adjournment_status"] == "open"
    assert by_cs[(119, "1")]["adjourned"] is None

    controls = pq.read_table(out / "generation" / f"{PARTY_CONTROL}.parquet").to_pylist()
    house_65 = next(row for row in controls if row["chamber"] == "house" and row["congress"] == 65)
    assert house_65["majority_basis"] == "house-seats-plurality"
    assert house_65["majority_party"] is None
    assert house_65["majority_seats"] == 215 and house_65["minority_seats"] == 214

    senate_68 = next(row for row in controls if row["chamber"] == "senate" and row["congress"] == 68)
    assert senate_68["printed_effective_from"] == date(1922, 12, 1)
    assert senate_68["effective_from"] == date(1924, 12, 1)
    assert senate_68["correction_reason"]

    senate_119 = next(row for row in controls if row["chamber"] == "senate" and row["congress"] == 119)
    assert senate_119["effective_to"] is None and senate_119["vacancies"] is None

    assert "congress-sessions" not in local_publication.LOCAL_FAMILIES
    from tests.generation_fakes import Store

    store = Store()
    published_index = publication.publish_generation(
        out / "generation",
        client=store,
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert published_index["families"][pipeline.FAMILY]["artifactDigest"] == artifact.pin.artifact_digest


def test_corrupt_pin_refuses_without_generation(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    selection = pipeline.acquire(
        tmp_path / "acq",
        archive_root=raw,
        transport=_transport(),
        min_request_interval_seconds=0.0,
    )
    selection["sources"]["house_session_dates"]["bytes"] += 1
    out = tmp_path / "bad-build"
    with pytest.raises(ValueError, match="pinned size or SHA-256"):
        pipeline.build(selection, out, archive_root=raw)
    assert not (out / "generation").exists()
