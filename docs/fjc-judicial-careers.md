# Follow Article III judicial appointments and service

The `fjc-judicial-careers` pipeline builds a local candidate from one declared
Federal Judicial Center flat export. SpicyDocs retains and verifies the complete
CSV; SpicyRegs exposes `fjc_judges`, `fjc_appointments`, `fjc_service_terms`,
`fjc_courts` and `fjc_snapshots` through the ordinary generation and SQL paths.

From this repository, with the coordinated SpicyDocs FJC reader available:

```sh
uv run python -m spicy_regs.pipelines.fjc acquire \
  --output-dir /path/to/owned/acquisition --captures-out /path/to/owned/captures.json
uv run python -m spicy_regs.pipelines.fjc build \
  --captures /path/to/owned/captures.json --output-dir /path/to/owned/build
```

Check the selected source catalog and retained originals before acquisition.
Replaying the same capture selection reads the central raw archive without HTTP;
use a fresh output directory. Select multiple capture files to preserve explicit
observations together. Each snapshot participates in every join. An absent later
row is unobserved, never inferred deleted. Publication uses the maintained local
family publisher after independent acceptance; building does not select shared
catalog or public state. Source-package release and integration are separate steps.

This query answers which courts Ruth Bader Ginsburg served on and traces each
service group back to exact FJC evidence:

```sql
SELECT j.nid, j.biography_url, a.service_ordinal, c.court_name_raw,
       a.appointment_kind, a.appointing_president_raw,
       a.commission_date, s.senior_status_date, s.termination_date,
       s.termination_reason_raw, s.vacancy_date_status,
       a.source_record_id, a.source_locator_json
FROM fjc_judges j
JOIN fjc_appointments a USING (snapshot_id, nid)
JOIN fjc_service_terms s USING
  (snapshot_id, nid, service_ordinal, appointment_id, court_label_id, seat_id_raw)
JOIN fjc_courts c USING (snapshot_id, court_label_id)
WHERE j.nid = '1381271'
ORDER BY a.service_ordinal;
```

The reviewed export states an appeals-court commission on June 18, 1980 and
termination on August 10, 1993, followed by a Supreme Court commission on
August 5, 1993 and termination by death on September 18, 2020. Those dates remain
separate. Changing the filter to `1376996` shows James Waldo Ackerman's Illinois
reassignment. Filtering `1386831` preserves William Rehnquist's two commissions on
the same court, including Associate Justice and Chief Justice appointment titles.

The appointment table has one observation per occupied service group, including
explicit `reassignment` and `assignment` kinds. These are not extra presidential
appointments. It preserves appointing and reappointing presidents and their parties
without claiming a judge's political affiliation. Source vote labels and counts
remain text; a voice vote has no inferred numeric share. Missing commissions in
unconfirmed recess appointments remain missing. Typed full dates retain original
strings and explicit parse status; partial or invalid dates remain unresolved.

Senior status does not end all judicial service. The source does not supply
vacancy dates; `vacancy_date` remains NULL with `not-provided-in-export` status.
Seat ID states succession, not a vacancy identity. Chief-judge periods retain
native years without invented months or days. Demographic terms remain the FJC's
literal historical or self-reported categories. Original values need no corrections
in the bounded reviewed sample.

The court table keys exact FJC type/name labels. Person joins use native `nid`;
legacy `jid` is separate. External judge and court identifiers remain NULL and
`unresolved`. Name equality never establishes a CourtListener relationship.
The selected population is the FJC's Article III directory, including its listed
historical courts and recess appointees. Other federal service, education, careers
and unsuccessful nominations remain in `raw_fields_json`, without additional
normalized tables. This does not deliver all federal judges, FJC cases, a complete
vacancy history, public acceptance or deployment.

See the lane's retained raw-review manifest, full-file reconciliation, ordinary
consumer responses, failure controls and exact-head handoff for qualification.
