"""Direct local delivery operations with pinned inputs and explicit read-only recovery."""

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from _mcp_readback import Client, LIMITS, require
from _image_delivery import ACCOUNT, REGISTRY

ROOT = Path(__file__).resolve().parents[2]
PUBLIC = "https://data.spicygov.ai"
SECTIONS = {
    "capture-publication": None,
    "push-image": "image",
    "verify-image": "image",
    "publish-data": "publication",
    "observe-publication": "publication",
    "readback": "readback",
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def read(pin):
    path = Path(pin["path"])
    require(path.is_absolute(), "Evidence paths must be absolute")
    require(sha(path) == pin["sha256"].removeprefix("sha256:"), f"Changed evidence: {path}")
    return json.loads(path.read_bytes())


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def source(pin):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()

    require(
        git("rev-parse", "HEAD") == pin["commit"]
        and git("rev-parse", "HEAD^{tree}") == pin["tree"]
        and not git("status", "--porcelain"),
        "Run from the exact clean reviewed source checkout",
    )
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT / "src"))


def publication_owner():
    from spicy_regs.sources import publication

    require(Path(publication.__file__).resolve().is_relative_to(ROOT / "src"), "Wrong publication implementation")
    return publication


def protected(index, prior, families):
    require(
        all(name in prior["families"] and index["families"].get(name) == prior["families"][name] for name in families),
        "Protected family changed; review before any retry",
    )


def image_inputs(config, source_pin):
    section = config["image"]
    qualified = read(section["qualification"])
    require(
        qualified["status"] == "pass"
        and qualified["source_commit"] == source_pin["commit"]
        and qualified["source_tree"] == source_pin["tree"],
        "Image qualification/source differs",
    )
    for name, digest in qualified["evidence"].items():
        path = Path(section["qualification"]["path"]).parent / name
        require(sha(path) == digest, "Image qualification evidence changed")
    build = section["build_inputs"]
    require(sha(build["path"]) == build["sha256"], "Build input list changed")
    for line in Path(build["path"]).read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        require(sha(ROOT / name) == digest, f"Build input changed: {name}")
    original = read(section["inspection"])[0]
    require(
        original["Id"] == qualified["image_id"]
        and original["Descriptor"]["digest"] == qualified["local_manifest_digest"]
        and original["Descriptor"]["annotations"]["config.digest"] == qualified["config_digest"],
        "Qualified image inspection differs",
    )
    tools = Path(section["tools"])
    require(
        sha(tools / "package-lock.json") == sha(ROOT / "deploy/cloudflare/package-lock.json"),
        "Wrangler lock differs from selected source",
    )
    require(
        json.loads((tools / "node_modules/wrangler/package.json").read_text())["version"] == "4.136.1",
        "Use the reviewed installed Wrangler 4.136.1",
    )
    return qualified, original


def publication_inputs(config):
    section = config["publication"]
    prior = publication_owner().parse_index(Path(section["prior"]["path"]).read_bytes())
    read(section["prior"])
    generation = section["generation"]
    root = json.loads((Path(generation["directory"]) / "artifact.json").read_bytes())
    require(
        root["artifactDigest"] == generation["artifact_digest"]
        and root["logicalId"] == generation["logical_id"]
        and root["spec"]["family"] == generation["family"]
        and set(root["spec"]["tables"]) == {name + ".parquet" for name in generation["tables"]},
        "Qualified generation differs",
    )
    require(generation["family"] not in section["protected_families"], "Target family cannot be protected")
    protected(prior, prior, section["protected_families"])
    return prior


def serving_inputs(config, source_pin):
    section = config["serving"]
    receipt = read(section["receipt"])
    association = read(section["association"])
    provider = read(section["provider_selection"])
    rollback = read(section["rollback"])
    require(
        rollback["status"] == "compatible-at-capture"
        and rollback["account_id"] == ACCOUNT
        and rollback["consumer"]["image_digest"] == rollback["image_digest"]
        and rollback["registry_image"] == f"{REGISTRY}@{rollback['image_digest']}"
        and rollback["receipt_sha256"]
        and rollback["worker_version"],
        "Compatible rollback evidence required",
    )
    require(
        association["status"] == "pass"
        and association["source_commit"] == source_pin["commit"]
        and association["source_tree"] == source_pin["tree"]
        and association["consumer"] == receipt["consumer"],
        "Receipt/registry/source association differs",
    )
    digest = receipt["consumer"]["image_digest"]
    require(
        association["registry_image"] == f"{REGISTRY}@{digest}"
        and provider["status"] == "pass"
        and provider["registry_image"] == association["registry_image"]
        and provider["receipt_sha256"] == "sha256:" + section["receipt"]["sha256"].removeprefix("sha256:")
        and provider["worker_version"]
        and provider["container_version"],
        "Actual provider selection does not bind the accepted image/receipt",
    )
    return receipt


def readback_inputs(config, receipt):
    section = config["readback"]
    prior = read(section["prior"])
    protected(prior, prior, section["protected_families"])
    queries = section["queries"]
    require(1 <= len(queries) <= 8, "Require one to eight bounded expected queries")
    for query in queries:
        require(
            isinstance(query["sql"], str)
            and query["sql"]
            and query["columns"]
            and query["publication"]
            and 1 <= len(query["rows"]) <= 100,
            "Exact bounded query rows/columns/publication required",
        )
        for name, binding in query["publication"].items():
            if binding.get("status") == "managed_generation":
                require(binding.get("family") and binding.get("artifact_digest"), "Managed query pin missing")
            else:
                release = binding.get("release_compatibility", {})
                require(
                    name in receipt["views"]
                    and release.get("status") == "compatible"
                    and release.get("receipt_sha256")
                    == "sha256:" + config["serving"]["receipt"]["sha256"].removeprefix("sha256:")
                    and all(
                        key in release and release[key] == receipt["views"][name][key]
                        for key in ("sql_sha256", "evidence_generations", "population", "as_of")
                    ),
                    "FEC query must bind the selected receipt/view evidence",
                )
    return prior


def public_index(prior, families):
    value = publication_owner().load_index(PUBLIC)
    protected(value, prior, families)
    return value


def capture_publication(output):
    from spicy_regs.runtime_bounds import RequestBound

    publication = publication_owner()
    url = f"{PUBLIC}/{publication.INDEX_V2_KEY}"
    with RequestBound(60, "Publication capture timeout").scope():
        raw = publication._bounded_get(
            url, allow_missing=False, headers={"Cache-Control": "no-cache"}, limit=publication.INDEX_LIMIT
        )
    path = output / "publication.raw.json"
    with path.open("xb") as stream:
        stream.write(raw)
    # Keep the original response even if its contents fail index validation.
    save(output / "publication.json", publication.parse_index(raw))
    return {
        "url": url,
        "prior": {"path": str(path.resolve()), "sha256": sha(path)},
        "byte_size": len(raw),
        "publication_write_requested": False,
    }


def publish_data(config, output, prior, *, observe=False):
    section = config["publication"]
    generation = section["generation"]
    family = generation["family"]
    publication = publication_owner()
    if not observe:
        receipt = read(config["serving"]["receipt"])
        referenced = {value["family"] for value in receipt["output_membership"].values()}
        for view in receipt["views"].values():
            referenced.update(value["family"] for value in view["dependencies"].values())
            referenced.update(view["evidence_generations"])
        require(
            family not in referenced,
            "Joint FEC input/receipt promotion requires the FEC release owner, not independent publish-data",
        )
        # Provider configuration cannot prove which image has actually started.
        # Refuse before reading credentials or writing any publication objects.
        readiness = {}
        with mcp_session(output, readiness) as client:
            check_consumer(client, config, receipt, readiness, "sources_ready")
    before = public_index(prior, section["protected_families"])
    save(output / "publication-before.json", before)
    if observe:
        from rulespec_artifacts import ArtifactPin, LocalMemberSource, iter_member_descriptors
        from spicy_regs.generations import admit_generation_for_publication

        artifact = admit_generation_for_publication(
            Path(generation["directory"]),
            expected_pin=ArtifactPin(generation["logical_id"], generation["artifact_digest"]),
        )
        expected = publication.generation_entry(
            artifact, iter_member_descriptors(artifact, LocalMemberSource(Path(generation["directory"])))
        )
        current = before["families"].get(family, {})
        require(
            {k: v for k, v in current.items() if k != "publishedAt"} == expected,
            "Candidate is not selected; observation performs no writes",
        )
        return {"family": current, "publication_write_requested": False}
    from dotenv import dotenv_values
    from spicy_regs.sources import r2

    require(Path(r2.__file__).resolve().is_relative_to(ROOT / "src"), "Wrong R2 implementation")
    credentials = dotenv_values(section["env_file"], interpolate=False)
    require(credentials.get("R2_ENDPOINT") == f"https://{ACCOUNT}.r2.cloudflarestorage.com", "Wrong R2 account")
    for key in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        require(bool(credentials.get(key)), f"Missing credential setting: {key}")
        os.environ[key] = credentials[key]
    os.environ["R2_BUCKET_NAME"] = "spicy-regs"
    # This owner retains full admission, exact member readback, current parents and family CAS.
    after = publication.publish_generation(
        Path(generation["directory"]),
        client=r2.get_r2_client(),
        bucket="spicy-regs",
        prior_index=prior,
        evidence_directories=tuple(Path(p) for p in generation["evidence_directories"]),
    )
    save(output / "selected-family.json", after["families"][family])
    protected(after, prior, section["protected_families"])
    observed = public_index(prior, section["protected_families"])
    save(output / "publication-after.json", observed)
    require(observed["families"][family] == after["families"][family], "Anonymous family readback differs")
    return {"family": observed["families"][family], "publication_write_requested": True}


def subset(actual, expected):
    """Require every explicitly requested publication field, allowing unrelated response metadata."""
    return (
        isinstance(actual, dict) and all(k in actual and subset(actual[k], v) for k, v in expected.items())
        if isinstance(expected, dict)
        else actual == expected
    )


@contextmanager
def mcp_session(output, result):
    """One bounded session, with failure evidence and cleanup shared by both checks."""
    client = Client("https://mcp.spicygov.ai/mcp", output, LIMITS)
    try:
        client.initialize()
        yield client
    finally:
        try:
            if client.session:
                result["cleanup"] = client.exchange(cleanup=True)
        finally:
            save(output / "http-records.json", {**result, "http_records": client.records})


def check_consumer(client, config, receipt, result, phase):
    selected = client.tool("list_sources", {}).get("fec_release", {})
    result[phase] = selected  # Retain the actual mismatch before refusing.
    require(
        selected.get("receipt_sha256")
        == "sha256:" + config["serving"]["receipt"]["sha256"].removeprefix("sha256:")
        and selected.get("consumer") == receipt["consumer"]
        and selected.get("status_counts") == {"compatible": len(receipt["views"])},
        "Running consumer/receipt/compatibility differs",
    )


def readback(config, output, receipt, prior):
    section = config["readback"]
    result = {}
    save(output / "publication-before.json", public_index(prior, section["protected_families"]))
    with mcp_session(output, result) as client:
        for phase in ("before", "after"):
            check_consumer(client, config, receipt, result, "sources_" + phase)
            if phase == "after":
                break
            for query in section["queries"]:
                value = client.tool(
                    "query_sql", {"sql": query["sql"], "max_rows": len(query["rows"]), "max_cell_chars": None}
                )
                require(
                    value.get("sql") == query["sql"]
                    and value.get("columns") == query["columns"]
                    and value.get("rows") == query["rows"]
                    and value.get("row_count_shown") == len(query["rows"])
                    and value.get("truncated") is False
                    and value.get("truncated_cells") == [],
                    "Exact complete query rows/columns differ",
                )
                require(
                    set(value.get("publication", {})) == set(query["publication"])
                    and subset(value["publication"], query["publication"]),
                    "Query publication binding differs",
                )
        save(output / "publication-after.json", public_index(prior, section["protected_families"]))
    return result


def execute(action, config, output, *, resume=False, from_output=None):
    source_pin = config["source"]
    source(source_pin)
    section = SECTIONS[action]
    selected = {"source": source_pin}
    if section is not None:
        selected[section] = config[section]
    if section in ("publication", "readback"):
        selected["serving"] = config["serving"]
    # Later receipt/provider population is deliberately absent from image input identity.
    inputs_sha = canonical(selected)
    implementation = {
        name: sha(Path(__file__).with_name(name))
        for name in ("local_delivery.py", "_image_delivery.py", "_mcp_readback.py")
    }
    if action == "capture-publication":
        prepared = None
    elif action in ("push-image", "verify-image"):
        prepared = image_inputs(config, source_pin)
    else:
        receipt = serving_inputs(config, source_pin)
        prepared = (
            publication_inputs(config)
            if action in ("publish-data", "observe-publication")
            else (receipt, readback_inputs(config, receipt))
        )
    if resume:
        terminal = json.loads((output / "terminal.json").read_bytes())
        require(
            terminal["status"] == "pass"
            and terminal["operation"] == action
            and terminal["inputs_sha256"] == inputs_sha
            and terminal["implementation"] == implementation,
            "No matching successful operation; use read-only observation for uncertain writes",
        )
        for name, digest in terminal["evidence"].items():
            require(sha(output / name) == digest, f"Retained operation evidence changed: {name}")
        return terminal
    if action in ("verify-image", "observe-publication"):
        require(from_output is not None, "Read-only recovery requires the original --from-output")
        intent = json.loads((from_output / "intent.json").read_bytes())
        original_action = "push-image" if action == "verify-image" else "publish-data"
        require(
            intent["operation"] == original_action and intent["inputs_sha256"] == inputs_sha,
            "Recovery inputs differ from original write intent",
        )
    else:
        require(from_output is None, "--from-output is only for read-only recovery")
    output.mkdir()  # Existing directories refuse, including interrupted/failed writes.
    started = datetime.now(UTC).isoformat()
    save(
        output / "intent.json",
        {
            "operation": action,
            "inputs_sha256": inputs_sha,
            "implementation": implementation,
            "inputs": selected,
            "started_at": started,
        },
    )
    terminal = {"status": "fail", "operation": action, "inputs_sha256": inputs_sha, "implementation": implementation}
    try:
        if action == "capture-publication":
            result = capture_publication(output)
        elif action in ("push-image", "verify-image"):
            from _image_delivery import image_operation

            result = image_operation(config["image"], source_pin, output, *prepared, push=action == "push-image")
        elif action in ("publish-data", "observe-publication"):
            result = publish_data(config, output, prepared, observe=action == "observe-publication")
        else:
            result = readback(config, output, *prepared)
        terminal.update(status="pass", result=result)
    except BaseException as error:
        # Transport exceptions can contain credentials. Keep type, not arbitrary exception text.
        terminal["failure_type"] = type(error).__name__
        raise
    finally:
        terminal["finished_at"] = datetime.now(UTC).isoformat()
        terminal["evidence"] = {p.name: sha(p) for p in output.iterdir() if p.is_file()}
        save(output / "terminal.json", terminal)
    return terminal


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=SECTIONS)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="One directory per operation; existing refuses")
    parser.add_argument("--resume", action="store_true", help="Return only a verified successful retained result")
    parser.add_argument("--from-output", type=Path, help="Original uncertain write; read-only recovery only")
    args = parser.parse_args(argv)
    try:
        value = execute(
            args.operation,
            json.loads(args.inputs.read_bytes()),
            args.output,
            resume=args.resume,
            from_output=args.from_output,
        )
    except Exception as error:
        parser.exit(1, f"{type(error).__name__}: operation refused; inspect retained evidence.\n")
    print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
