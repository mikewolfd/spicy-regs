"""Translate retained H/D preparations into reviewable future requests, never grants."""

from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys

from spicy_regs.operational_runner import durable, file_pin, pinned_bytes, read_json, require


def canonical(pin):
    return {"path": pin["path"], "bytes": pin["bytes"], "sha256": "sha256:" + pin["sha256"].removeprefix("sha256:")}


def unique(pins):
    result = {}
    for item in pins:
        pin = canonical(item)
        require(pin["path"] not in result or result[pin["path"]] == pin, "Conflicting inventory pin: " + pin["path"])
        result[pin["path"]] = pin
    return sorted(result.values(), key=lambda item: item["path"])


def prepare_request(args):
    """Use existing inventories; supplement actual loaded runner dependencies.

    No source data is read. A changed installed target/runtime needs its own
    inventory, then the resulting exact request needs the existing owner grant.
    Historical selection and accepted evidence files are never rewritten.
    """
    require(not args.output.exists() and not args.output.is_symlink(), "Fresh request output required")
    adapter_path = args.output.with_name(args.output.stem + "-source.json")
    require(not adapter_path.exists(), "Fresh source adapter configuration required")
    old = read_json(args.preparation)
    inventory = read_json(args.runtime_inventory)
    runtime = list(inventory["files"])
    inputs = [file_pin(args.preparation.resolve()), file_pin(args.runtime_inventory.resolve())]
    target = args.target.resolve()
    require(
        (target / "spicy_regs/operational_sources.py").is_file(),
        "Install maintained runner/adapter in a new target first",
    )
    require(
        str(Path(args.python).resolve()) == str(Path(sys.executable).resolve()),
        "Prepare using the selected runtime interpreter",
    )
    # These small additions avoid manually enumerating the new runner's stdlib
    # and psutil dependencies. They are preparation observations, not acceptance.
    for module in tuple(sys.modules.values()):
        path = getattr(module, "__file__", None)
        if path and not path.startswith("<"):
            runtime.append(file_pin(Path(path).resolve()))
    runtime.append(file_pin(Path(args.python).resolve()))
    # The whole first-party package is code: the builder/auditor read its reader files and the
    # offline guard refuses any read that is not pinned.
    code = [
        file_pin(path)
        for path in sorted((target / "spicy_regs").rglob("*"))
        if path.is_file() and "__pycache__" not in path.relative_to(target).parts
    ]
    if args.kind == "h-annual":
        require(old["format"] == "H-maintained-annual1994-source-replay-request-v1", "Unsupported H preparation")
        config = {key: old[key] for key in ("selection", "original_input", "conservation")}
        config["selection"], config["original_input"] = (
            canonical(config["selection"]),
            canonical(config["original_input"]),
        )
        config["annual_year"] = old["original_input"]["year"]
        recipe_code = """
import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from spicy_regs.sources import uscode_editions as builder, uscode_editions_audit as auditor
pins=builder._code_pins()
assert pins==auditor.reader_pins(), 'Builder/auditor reader recipe differs'
generation=builder._identity('uscode-editions-public-v2',builder._digest(Path(sys.argv[2]).read_bytes()),pins)
print(json.dumps({'expected_reader_pins':pins,'expected_generation':generation}))
"""
        config.update(
            json.loads(
                subprocess.check_output(
                    [args.python, "-I", "-B", "-c", recipe_code, str(target), config["selection"]["path"]], text=True
                )
            )
        )
        config["historical_code_reference"] = {
            "reader_pins": old["expected_reader_pins"],
            "generation": old["expected_generation"],
            "scope": "Reference only; new target recipe requires review, source selection unchanged",
        }
        inputs += old["metadata_pins"] + [old["selection"], old["original_input"]]
        bounds = old["resource_request"]
        limits = {
            "wall_seconds": bounds["wall_seconds"],
            "interval_seconds": bounds["sample_interval_seconds"],
            "rss_bytes": bounds["process_tree_rss_bytes"],
            "output_bytes": bounds["owned_output_scratch_bytes"],
            "start_free_bytes": bounds["admission_free_bytes"],
            "stop_free_bytes": bounds["stop_free_bytes"],
        }
        phases = ("h-build", "h-audit")
    else:
        require(old["phase"] in ("preflight", "semantic"), "Unsupported D preparation")
        # One grant covers this run's preflight then semantic phase; the semantic phase
        # requires the preflight record this same run sealed.
        config = {"capture_config": canonical(old["capture_config"])}
        inputs.append(config["capture_config"])
        capture = json.loads(pinned_bytes(config["capture_config"]))  # The pin is checked before it is trusted.
        inputs += capture["custody_acceptance"]
        for spec in capture["bodies"].values():
            inputs.append(spec["stored"])
        inputs += [item["capture"] for item in capture["contexts"]]
        bounds = old["limits"]
        limits = {
            key: bounds[key]
            for key in ("wall_seconds", "interval_seconds", "rss_bytes", "start_free_bytes", "stop_free_bytes")
        }
        limits.update(
            output_bytes=bounds["aggregate_bytes"],
            scratch_bytes=bounds["scratch_bytes"],
            data_bytes=bounds["output_bytes"],
            evidence_bytes=bounds["evidence_bytes"],
        )
        phases = ("d-preflight", "d-semantic")
    for phase in phases:
        require(phase in ("h-build", "h-audit", "d-preflight", "d-semantic"), "Fixed source phase required")
    inputs, runtime, code = unique(inputs), unique(runtime), unique(code)
    # Inventories pin files that may no longer exist; refuse now rather than at admission.
    for kind, pins, advice in (
        ("runtime", runtime, "produce a new runtime inventory for --python"),
        (
            "input",
            inputs,
            "the preparation references missing bodies: produce a new "
            "capture config or selection from the current corpus",
        ),
    ):
        missing = [pin["path"] for pin in pins if not Path(pin["path"]).is_file()]
        require(not missing, f"{len(missing)} of {len(pins)} {kind} pin file(s) missing, first {missing[:5]}; {advice}")
    durable(adapter_path, config)
    inputs = unique([*inputs, file_pin(adapter_path.resolve())])
    request = {
        "version": 1,
        "run_root": str(args.run_root.resolve()),
        "python": args.python,
        "inputs": inputs,
        "runtime": runtime,
        "code": code,
        "activation": {
            "prefix": str(Path(sys.prefix).resolve()),
            "paths": [str(target)],
            "base_paths": json.loads(
                subprocess.check_output(
                    [
                        args.python,
                        "-I",
                        "-B",
                        "-c",
                        "import json,sys; from pathlib import Path; print(json.dumps([str(Path(p).resolve()) for p in sys.path]))",
                    ],
                    text=True,
                )
            ),
            "origins": {"spicy_regs": str(target / "spicy_regs/__init__.py")},
        },
        "offline_single_process": True,
        "limits": limits,
        "commands": {
            name: {
                "module": "spicy_regs.operational_sources",
                "args": [
                    phase,
                    "--config",
                    str(adapter_path.resolve()),
                    "--candidate",
                    "{candidate}",
                    *(["--output", "{run}/output/audit.json"] if name == "audit" else []),
                ],
            }
            for name, phase in zip(("build", "audit"), phases, strict=True)
        },
    }
    durable(args.output, request)
    return request
