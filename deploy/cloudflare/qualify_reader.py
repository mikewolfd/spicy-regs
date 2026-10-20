"""Check an installed reader using explicit local inputs and expected query rows."""

import argparse
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import runpy
import sys
import sysconfig
import time


def read_expected(path, digest):
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest.removeprefix("sha256:"):
        raise ValueError("Reader expectations changed")
    value = json.loads(raw)
    if not value.get("queries") or len(value["queries"]) > 8:
        raise ValueError("Require one to eight expected queries")
    for query in value["queries"]:
        if not query["sql"] or not query["columns"] or not isinstance(query["rows"], list) or len(query["rows"]) > 100:
            raise ValueError("Require bounded expected query rows and columns")
    if value.get("pipeline") and Path(value["generation"]["directory"]).exists():
        raise ValueError("Pipeline check requires a new generation directory")
    if not value.get("pipeline") and (
        not all(key in value["generation"] for key in ("logicalId", "artifactDigest"))
        or not value.get("consumer") or not value.get("mcp_source_sha256")
    ):
        raise ValueError("Existing generation requires exact generation and consumer identity")
    for pin in value.get("inputs", []):
        if hashlib.sha256(Path(pin["path"]).read_bytes()).hexdigest() != pin["sha256"].removeprefix("sha256:"):
            raise ValueError("Reader input changed")
    return value


def qualify(expected):
    started = time.monotonic()
    network_attempts = []

    def refuse_network(event, _args):
        if event in {"socket.connect", "socket.getaddrinfo"}:
            network_attempts.append(event)
            raise RuntimeError("Installed reader check attempted network access")

    sys.addaudithook(refuse_network)
    if expected.get("require_docs_absent") and importlib.util.find_spec("spicy_docs") is not None:
        raise ValueError("Serving reader unexpectedly includes spicy-docs")
    pipeline = expected.get("pipeline")
    pipeline_stdout = None
    if pipeline:
        previous = sys.argv
        sys.argv = [pipeline["module"], *pipeline["argv"]]
        captured = io.StringIO()
        try:
            with redirect_stdout(captured):
                runpy.run_module(pipeline["module"], run_name="__main__")
        finally:
            sys.argv = previous
        pipeline_stdout = captured.getvalue()

    from spicy_regs import fec_release, mcp_server

    directory = Path(expected["generation"]["directory"])
    artifact = json.loads((directory / "artifact.json").read_bytes())
    for key in ("logicalId", "artifactDigest"):
        if key in expected["generation"] and artifact[key] != expected["generation"][key]:
            raise ValueError("Reader generation differs")
    consumer = fec_release.runtime_consumer(None)
    if "consumer" in expected and consumer != expected["consumer"]:
        raise ValueError("Installed consumer differs")
    if "mcp_source_sha256" in expected:
        if hashlib.sha256(Path(mcp_server.__file__).read_bytes()).hexdigest() != expected["mcp_source_sha256"]:
            raise ValueError("Installed MCP source differs")
    mcp_server.DATA_DIR = directory
    results = []
    with mcp_server._build_connection() as connection:
        for query in expected["queries"]:
            cursor = connection.execute(query["sql"])
            columns = [column[0] for column in cursor.description]
            # A wrong query must not materialize an unexpectedly large result.
            rows = mcp_server._jsonify(cursor.fetchmany(len(query["rows"]) + 1))
            if columns != query["columns"] or rows != query["rows"]:
                raise ValueError("Reader query differs from expected complete rows")
            results.append({"sql": query["sql"], "columns": columns, "rows": rows})
    package_root = Path(sysconfig.get_paths()["purelib"]).resolve()
    origins = {
        name: str(Path(module.__file__).resolve())
        for name, module in sys.modules.items()
        if name.split(".")[0] in {"spicy_regs", "spicy_docs"} and getattr(module, "__file__", None)
    }
    if not origins or not all(Path(path).is_relative_to(package_root) for path in origins.values()):
        raise ValueError("Reader imported project modules outside the installed runtime")
    if network_attempts:
        raise ValueError("Reader attempted Python socket access")
    return {
        "status": "pass",
        "consumer": consumer,
        "generation": artifact["artifactDigest"],
        "queries": results,
        "pipeline_stdout": pipeline_stdout,
        "module_origins": origins,
        "python_socket_attempts": network_attempts,
        "elapsed_seconds": time.monotonic() - started,
        "scope": "Installed local pipeline/reader check. Container callers additionally enforce --network none.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()
    result = qualify(read_expected(args.expected, args.sha256))
    result["expected_sha256"] = args.sha256.removeprefix("sha256:")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
