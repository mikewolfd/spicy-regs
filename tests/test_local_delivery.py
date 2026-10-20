"""Local delivery controls refuse drift and never replay uncertain writes."""

from copy import deepcopy
import json
from pathlib import Path
import socket
import sys
from types import SimpleNamespace

import pytest

HELPERS = Path(__file__).resolve().parents[1] / "deploy/cloudflare"
sys.path.insert(0, str(HELPERS))
import local_delivery as delivery  # noqa: E402
import _image_delivery as image  # noqa: E402
import _mcp_readback as mcp  # noqa: E402


def pin(path, value):
    path.write_text(json.dumps(value))
    return {"path": str(path), "sha256": delivery.sha(path)}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: pytest.fail("Network forbidden"))
    monkeypatch.setattr(delivery, "source", lambda value: None)


@pytest.fixture
def release(tmp_path, monkeypatch):
    monkeypatch.setattr(delivery, "ROOT", tmp_path)
    (tmp_path / "uv.lock").write_text("lock")
    cloudflare = tmp_path / "deploy/cloudflare"
    cloudflare.mkdir(parents=True)
    (cloudflare / "package-lock.json").write_text("tool lock")
    package = cloudflare / "node_modules/wrangler/package.json"
    package.parent.mkdir(parents=True)
    package.write_text('{"version":"4.136.1"}')
    build = tmp_path / "build-inputs.sha256"
    build.write_text(delivery.sha(tmp_path / "uv.lock") + " uv.lock\n")
    config_digest = "sha256:" + "c" * 64
    media_type = "application/vnd.oci.image.manifest.v1+json"
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": media_type,
            "config": {"digest": config_digest},
            "layers": [{"digest": "layer"}],
        }
    ).encode()
    digest = "sha256:" + image.hashlib.sha256(manifest).hexdigest()
    consumer = {"code_sha256": "sha256:code", "image_digest": digest, "package_versions": {"spicy-regs": "0.1.6"}}
    original = {
        "Id": digest,
        "Descriptor": {
            "digest": digest,
            "size": len(manifest),
            "mediaType": media_type,
            "annotations": {"config.digest": config_digest},
        },
        "RootFS": {"Layers": ["diff1"]},
        "Architecture": "amd64",
        "Os": "linux",
        "Config": {"Env": []},
        "Created": "today",
    }
    qualification = {
        "status": "pass",
        "source_commit": "commit",
        "source_tree": "tree",
        "image_id": original["Id"],
        "local_manifest_digest": digest,
        "config_digest": config_digest,
        "evidence": {},
        "consumer": {**consumer, "image_digest": None},
    }
    receipt = {
        "consumer": consumer,
        "output_membership": {"fec_table": {"family": "fec-query"}},
        "views": {"fec_view": {"dependencies": {}, "evidence_generations": {}}},
    }
    receipt_pin = pin(tmp_path / "receipt.json", receipt)
    association = {
        "status": "pass",
        "source_commit": "commit",
        "source_tree": "tree",
        "consumer": consumer,
        "registry_image": delivery.REGISTRY + "@" + digest,
    }
    provider = {
        "status": "pass",
        "registry_image": association["registry_image"],
        "receipt_sha256": "sha256:" + receipt_pin["sha256"],
        "worker_version": "worker",
        "container_version": 57,
    }
    prior = {"families": {"protected": {"artifactDigest": "old"}}}
    prior_pin = pin(tmp_path / "prior.json", prior)
    rollback = pin(
        tmp_path / "rollback.json",
        {
            "status": "compatible-at-capture",
            "account_id": delivery.ACCOUNT,
            "worker_version": "previous",
            "receipt_sha256": "old",
            "image_digest": "old",
            "consumer": {"image_digest": "old"},
            "registry_image": delivery.REGISTRY + "@old",
        },
    )
    generation = tmp_path / "generation"
    generation.mkdir()
    (generation / "artifact.json").write_text(
        json.dumps(
            {
                "artifactDigest": "sha256:new",
                "logicalId": "sample",
                "spec": {"family": "sample", "tables": {"sample.parquet": {}}},
            }
        )
    )
    config = {
        "source": {"commit": "commit", "tree": "tree"},
        "image": {
            "qualification": pin(tmp_path / "qualification.json", qualification),
            "inspection": pin(tmp_path / "inspection.json", [original]),
            "build_inputs": {"path": str(build), "sha256": delivery.sha(build)},
            "tools": str(cloudflare),
            "docker": "/fake/docker",
            "node": "/fake/node",
            "docker_host": "unix:///fake.sock",
        },
        "publication": {
            "prior": prior_pin,
            "rollback": rollback,
            "protected_families": ["protected"],
            "env_file": str(tmp_path / ".env"),
            "generation": {
                "directory": str(generation),
                "logical_id": "sample",
                "artifact_digest": "sha256:new",
                "family": "sample",
                "tables": ["sample"],
                "evidence_directories": [],
            },
        },
        "serving": {
            "receipt": receipt_pin,
            "association": pin(tmp_path / "association.json", association),
            "provider_selection": pin(tmp_path / "provider.json", provider),
            "rollback": rollback,
        },
        "readback": {
            "prior": prior_pin,
            "protected_families": ["protected"],
            "queries": [
                {
                    "sql": "SELECT id FROM sample LIMIT 1",
                    "columns": ["id"],
                    "rows": [["literal"]],
                    "publication": {
                        "sample": {"status": "managed_generation", "family": "sample", "artifact_digest": "sha256:new"}
                    },
                }
            ],
        },
    }
    return config, qualification, original, manifest


def test_image_resume_checks_retained_bytes_but_later_receipt_does_not_repush(release, tmp_path, monkeypatch):
    config, *_ = release
    calls = []

    def push(*args, **kwargs):
        calls.append(kwargs["push"])
        (args[2] / "registry-raw.json").write_text("verified")
        return {"registry_image": "qualified"}

    monkeypatch.setattr(image, "image_operation", push)
    output = tmp_path / "push"
    first = delivery.execute("push-image", config, output)
    config["serving"]["receipt"] = None
    assert delivery.execute("push-image", config, output, resume=True) == first
    assert calls == [True]
    (output / "registry-raw.json").write_text("changed")
    with pytest.raises(ValueError, match="Retained operation evidence changed"):
        delivery.execute("push-image", config, output, resume=True)
    assert calls == [True]


@pytest.mark.parametrize("which", ["qualification", "inspection", "build_inputs"])
def test_changed_image_controls_refuse_before_effects(release, tmp_path, monkeypatch, which):
    config, *_ = release
    Path(config["image"][which]["path"]).write_text("changed")
    monkeypatch.setattr(image, "image_operation", lambda *a, **k: pytest.fail("Unexpected push"))
    with pytest.raises(ValueError):
        delivery.execute("push-image", config, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_uncertain_push_requires_read_only_observation(release, tmp_path, monkeypatch):
    config, *_ = release
    calls = []

    def effect(*args, push):
        calls.append(push)
        if push:
            raise OSError("secret transport text must not be retained")
        return {"status": "pass"}

    monkeypatch.setattr(image, "image_operation", effect)
    output = tmp_path / "push"
    with pytest.raises(OSError):
        delivery.execute("push-image", config, output)
    with pytest.raises(ValueError, match="No matching successful"):
        delivery.execute("push-image", config, output, resume=True)
    with pytest.raises(FileExistsError):
        delivery.execute("push-image", config, output)
    assert "secret transport" not in (output / "terminal.json").read_text()
    delivery.execute("verify-image", config, tmp_path / "observe", from_output=output)
    assert calls == [True, False]
    changed = deepcopy(config)
    changed["image"]["docker_host"] = "unix:///other.sock"
    with pytest.raises(ValueError, match="Recovery inputs differ"):
        delivery.execute("verify-image", changed, tmp_path / "wrong", from_output=output)


@pytest.mark.parametrize("bad", ["receipt", "provider", "source", "rows", "missing"])
def test_readback_preconditions_refuse_before_client(release, tmp_path, monkeypatch, bad):
    config, *_ = release
    section = config["serving"]
    if bad == "receipt":
        section["receipt"]["sha256"] = "0" * 64
    elif bad == "provider":
        value = delivery.read(section["provider_selection"])
        value["registry_image"] = "configured-not-selected"
        section["provider_selection"] = pin(tmp_path / "wrong-provider.json", value)
    elif bad == "source":
        config["source"]["commit"] = "other"
    elif bad == "rows":
        config["readback"]["queries"][0]["rows"] = []
    else:
        del section["association"]
    monkeypatch.setattr(delivery, "Client", lambda *a: pytest.fail("Unexpected client"))
    with pytest.raises((ValueError, KeyError)):
        delivery.execute("readback", config, tmp_path / "out")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("bad", [None, "rows", "generation", "consumer", "truncated", "cleanup"])
def test_readback_exact_query_and_cleanup(release, tmp_path, monkeypatch, bad):
    config, *_ = release
    section = config["readback"]
    receipt = delivery.read(config["serving"]["receipt"])
    query = section["queries"][0]
    instances = []

    class FakeClient:
        def __init__(self, *args):
            self.session, self.records, self.cleaned = "session", [], False
            instances.append(self)

        def initialize(self):
            pass

        def tool(self, name, args):
            if name == "list_sources":
                return {
                    "fec_release": {
                        "consumer": {} if bad == "consumer" else receipt["consumer"],
                        "receipt_sha256": "sha256:" + config["serving"]["receipt"]["sha256"],
                        "status_counts": {"compatible": 1},
                    }
                }
            assert args == {"sql": query["sql"], "max_rows": 1, "max_cell_chars": None}
            result = {**deepcopy(query), "row_count_shown": 1, "truncated": False, "truncated_cells": []}
            if bad == "rows":
                result["rows"] = [["different"]]
            elif bad == "generation":
                result["publication"]["sample"]["artifact_digest"] = "other"
            elif bad == "truncated":
                result["truncated"] = True
            return result

        def exchange(self, *, cleanup):
            assert cleanup
            self.cleaned = True
            if bad == "cleanup":
                raise ValueError("cleanup failed")
            return {"terminated": True}

    monkeypatch.setattr(delivery, "Client", FakeClient)
    monkeypatch.setattr(delivery, "public_index", lambda *a: delivery.read(section["prior"]))
    output = tmp_path / "readback"
    if bad:
        with pytest.raises(ValueError):
            delivery.execute("readback", config, output)
        assert json.loads((output / "terminal.json").read_text())["status"] == "fail"
    else:
        assert delivery.execute("readback", config, output)["status"] == "pass"
    assert instances[0].cleaned


def test_publication_owner_arguments_resume_and_uncertain_observation(release, tmp_path, monkeypatch):
    config, *_ = release
    prior = delivery.read(config["publication"]["prior"])
    calls = []
    monkeypatch.setattr(delivery, "publication_owner", lambda: SimpleNamespace(parse_index=json.loads))

    def publish(config, output, actual_prior, *, observe):
        assert actual_prior == prior
        calls.append(observe)
        if not observe:
            raise OSError("ambiguous pointer response")
        return {"publication_write_requested": False}

    monkeypatch.setattr(delivery, "publish_data", publish)
    with pytest.raises(OSError):
        delivery.execute("publish-data", config, tmp_path / "write")
    with pytest.raises(ValueError):
        delivery.execute("publish-data", config, tmp_path / "write", resume=True)
    result = delivery.execute("observe-publication", config, tmp_path / "observed", from_output=tmp_path / "write")
    assert not result["result"]["publication_write_requested"]
    assert calls == [False, True]


def test_missing_protected_family_refuses():
    with pytest.raises(ValueError, match="Protected family"):
        delivery.protected({"families": {}}, {"families": {"treasury": {"digest": "a"}}}, ["treasury"])


@pytest.mark.parametrize("post_push", ["same", "missing-digest", "changed-config"])
def test_image_push_exact_registry_association_and_no_credential_output(release, tmp_path, monkeypatch, post_push):
    config, qualified, original, manifest = release
    calls = []
    auth_paths = []

    def run(args, section, env, output, name, **kwargs):
        calls.append(args)
        assert "CLOUDFLARE_API_TOKEN" not in env and "CF_API_TOKEN" not in env
        if "whoami" in args:
            assert args[-2:] == ["whoami", "--json"] and "--profile" not in args
            value = {
                "loggedIn": True,
                "accounts": [{"id": image.ACCOUNT}],
                "tokenPermissions": ["containers:write", "cloudchamber:write"],
            }
        elif "push" in args:
            private = Path(env["DOCKER_CONFIG"])
            auth_paths.append(private)
            assert not private.is_relative_to(output)
            assert private.stat().st_mode & 0o777 == 0o700
            assert (private / "config.json").stat().st_mode & 0o777 == 0o600
            assert args[-4:] == ["--profile", image.PROFILE, "--path-to-docker", "/fake/docker"]
            return b"pushed"
        elif "--raw" in args:
            (output / name).write_bytes(manifest)
            return manifest
        elif "--format" in args:
            value = {
                "architecture": "amd64",
                "os": "linux",
                "rootfs": {"diff_ids": ["diff1"]},
                "config": original["Config"],
                "created": original["Created"],
            }
        elif name == "pushed-image-inspect.json":
            inspected = {**original, "RepoDigests": [image.REGISTRY + "@" + qualified["local_manifest_digest"]]}
            if post_push == "missing-digest":
                inspected["RepoDigests"] = []
            elif post_push == "changed-config":
                inspected["Config"] = {"Env": ["changed"]}
            value = [inspected]
        else:
            value = [original]
        raw = json.dumps(value).encode()
        (output / name).write_bytes(raw)
        return raw

    monkeypatch.setattr(image, "command", run)
    monkeypatch.setenv("CF_API_TOKEN", "never retain")
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "never retain")
    output = tmp_path / "image"
    output.mkdir()
    if post_push == "same":
        value = image.image_operation(config["image"], config["source"], output, qualified, original, push=True)
        assert value["consumer"]["image_digest"] == qualified["local_manifest_digest"]
        assert value["uv_lock_sha256"] == delivery.sha(tmp_path / "uv.lock")
        assert (output / "registry-manifest.json").read_bytes() == manifest
        assert not (output / "registry-manifest-raw.json").exists()
        assert json.loads((output / "pushed-image-inspect.json").read_bytes())[0]["RepoDigests"] == [
            value["registry_image"]
        ]
        assert "never retain" not in (output / "registry-association.json").read_text()
    else:
        with pytest.raises(ValueError, match="exact pushed registry digest|Post-push qualified image changed"):
            image.image_operation(config["image"], config["source"], output, qualified, original, push=True)
        assert not (output / "registry-association.json").exists()
    assert sum("push" in args for args in calls) == 1
    assert sum(args[1:3] == ["image", "inspect"] for args in calls) == 2
    assert len(auth_paths) == 1 and not auth_paths[0].exists()
    assert not list(output.glob("docker-auth-*"))


# The moved client must preserve bounded HTTP behavior without contacting a server.
@pytest.mark.parametrize("case", ["json", "redirect", "cap", "expired", "wrong-id", "sse", "cleanup"])
def test_bounded_client(case, tmp_path, monkeypatch):
    reply = {"jsonrpc": "2.0", "id": 7 if case == "wrong-id" else 1, "result": {"ok": True}}
    raw = json.dumps(reply).encode()
    if case == "sse":
        raw = b"data: " + raw + b"\n\n"
    status = 302 if case == "redirect" else 404 if case == "expired" else 405 if case == "cleanup" else 200

    class Response:
        def __init__(self):
            self.status, self.read = status, False

        def getheader(self, name, default=None):
            return {"Content-Type": "text/event-stream" if case == "sse" else "application/json"}.get(name, default)

        def isclosed(self):
            return self.read

        def read1(self, count):
            self.read = True
            return raw[:count]

    class Connection:
        def __init__(self, *a, **k):
            self.sock = SimpleNamespace(settimeout=lambda value: None)

        def request(self, *a, **k):
            pass

        def getresponse(self):
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(mcp.http.client, "HTTPSConnection", Connection)
    limits = {**mcp.LIMITS, "response_bytes": 5} if case == "cap" else mcp.LIMITS
    client = mcp.Client("https://mcp.spicygov.ai/mcp", tmp_path, limits)
    if case in ("expired", "cleanup"):
        client.session = "existing"
    if case == "cleanup":
        assert client.exchange(cleanup=True)["server_disallows_delete"]
    elif case in ("redirect", "cap", "expired", "wrong-id"):
        with pytest.raises(ValueError):
            client.request("test", {})
    else:
        assert client.request("test", {}) == {"ok": True}
    assert len(client.records) == 1


def test_readonly_registry_auth_uses_pull_only_and_keeps_secrets_private(release, tmp_path, monkeypatch):
    config, qualified, original, manifest = release
    calls = []
    auth_paths = []

    def run(args, section, env, output, name, **kwargs):
        assert "push" not in args
        if "whoami" in args:
            value = {
                "loggedIn": True,
                "accounts": [{"id": image.ACCOUNT}],
                "tokenPermissions": ["containers:write", "cloudchamber:write"],
            }
        elif "--raw" in args:
            return manifest
        elif "--format" in args:
            value = {
                "architecture": "amd64",
                "os": "linux",
                "rootfs": {"diff_ids": ["diff1"]},
                "config": original["Config"],
                "created": original["Created"],
            }
        elif name == "pushed-image-inspect.json":
            value = [{**original, "RepoDigests": [image.REGISTRY + "@" + qualified["local_manifest_digest"]]}]
        else:
            value = [original]
        return json.dumps(value).encode()

    def private(args, **kwargs):
        calls.append(args)
        assert kwargs["capture_output"]
        if "credentials" in args:
            private = Path(kwargs["env"]["DOCKER_CONFIG"])
            auth_paths.append(private)
            assert not private.is_relative_to(output)
            assert private.stat().st_mode & 0o777 == 0o700
            assert (private / "config.json").stat().st_mode & 0o777 == 0o600
            assert args[-7:] == [
                "containers",
                "registries",
                "credentials",
                "--pull",
                "--json",
                "--profile",
                image.PROFILE,
            ]
            return SimpleNamespace(returncode=0, stdout=b'{"username":"ephemeral","password":"secret-value"}')
        assert args == [
            "/fake/docker",
            "login",
            "--password-stdin",
            "--username",
            "ephemeral",
            "registry.cloudflare.com",
        ]
        assert kwargs["input"] == b"secret-value"
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(image, "command", run)
    monkeypatch.setattr(image.subprocess, "run", private)
    output = tmp_path / "verify"
    output.mkdir()
    result = image.image_operation(config["image"], config["source"], output, qualified, original, push=False)
    assert result["push_requested"] is False and len(calls) == 2
    assert len(auth_paths) == 1 and not auth_paths[0].exists()
    assert not list(output.glob("docker-auth-*"))
    assert all(b"secret-value" not in p.read_bytes() for p in output.iterdir())


@pytest.mark.parametrize("fail", [False, True])
def test_publication_delegates_full_owner_and_preserves_prior(release, tmp_path, monkeypatch, fail):
    from spicy_regs.sources import r2
    import dotenv

    config, *_ = release
    prior = delivery.read(config["publication"]["prior"])
    current = deepcopy(prior)
    calls = []
    readiness = []
    install_readiness_client(monkeypatch, config, readiness)
    client = object()

    def publish(directory, **kwargs):
        assert readiness == ["initialize", "list_sources", "cleanup"]
        calls.append((directory, kwargs))
        assert kwargs == {"client": client, "bucket": "spicy-regs", "prior_index": prior, "evidence_directories": ()}
        if fail:
            raise ValueError("Owner CAS refusal")
        current["families"]["sample"] = {"artifactDigest": "sha256:new"}
        return deepcopy(current)

    owner = SimpleNamespace(
        parse_index=json.loads, load_index=lambda url: deepcopy(current), publish_generation=publish
    )
    monkeypatch.setattr(delivery, "publication_owner", lambda: owner)
    monkeypatch.setattr(r2, "__file__", str(tmp_path / "src/spicy_regs/sources/r2.py"))
    monkeypatch.setattr(r2, "get_r2_client", lambda: client)
    monkeypatch.setattr(
        dotenv,
        "dotenv_values",
        lambda *a, **k: {
            "R2_ENDPOINT": f"https://{delivery.ACCOUNT}.r2.cloudflarestorage.com",
            "R2_ACCESS_KEY_ID": "fake-key",
            "R2_SECRET_ACCESS_KEY": "fake-secret",
        },
    )
    for key in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"):
        monkeypatch.setenv(key, "original")  # Restore all test-environment mutations afterwards.
    output = tmp_path / "publication"
    if fail:
        with pytest.raises(ValueError, match="Owner CAS refusal"):
            delivery.execute("publish-data", config, output)
        assert current == prior
    else:
        value = delivery.execute("publish-data", config, output)
        assert value["status"] == "pass"
        assert delivery.execute("publish-data", config, output, resume=True) == value
    assert len(calls) == 1
    assert all(b"fake-secret" not in p.read_bytes() for p in output.iterdir())


def install_readiness_client(monkeypatch, config, calls, failure=None):
    receipt = delivery.read(config["serving"]["receipt"])

    class ReadyClient:
        def __init__(self, *args):
            self.session, self.records = "session", []

        def initialize(self):
            calls.append("initialize")

        def tool(self, name, args):
            calls.append(name)
            assert name == "list_sources" and args == {}
            if failure == "timeout":
                raise TimeoutError("readiness deadline")
            selected = {
                "consumer": deepcopy(receipt["consumer"]),
                "receipt_sha256": "sha256:" + config["serving"]["receipt"]["sha256"],
                "status_counts": {"compatible": len(receipt["views"])},
            }
            if failure == "code":
                selected["consumer"]["code_sha256"] = "old-code"
            elif failure == "receipt":
                selected["receipt_sha256"] = "old-receipt"
            elif failure == "disabled":
                selected["status_counts"] = {"disabled": 1}
            elif failure == "unknown":
                selected = {}
            return {"fec_release": selected}

        def exchange(self, *, cleanup):
            assert cleanup
            calls.append("cleanup")
            if failure == "cleanup":
                raise OSError("session cleanup failed")
            return {"terminated": True}

    monkeypatch.setattr(delivery, "Client", ReadyClient)


@pytest.mark.parametrize("failure", ["code", "receipt", "disabled", "unknown", "timeout", "cleanup"])
def test_readiness_failure_retains_diagnosis_before_credentials_or_publication(
    release, tmp_path, monkeypatch, failure
):
    import dotenv

    config, *_ = release
    calls = []
    install_readiness_client(monkeypatch, config, calls, failure)
    monkeypatch.setattr(delivery, "publication_owner", lambda: SimpleNamespace(parse_index=json.loads))
    monkeypatch.setattr(dotenv, "dotenv_values", lambda *a, **k: pytest.fail("Credentials read before readiness"))
    output = tmp_path / "refused"
    with pytest.raises((ValueError, TimeoutError, OSError)):
        delivery.execute("publish-data", config, output)
    assert calls == ["initialize", "list_sources", "cleanup"]
    assert json.loads((output / "terminal.json").read_text())["status"] == "fail"
    diagnostic = json.loads((output / "http-records.json").read_text())
    if failure == "code":
        assert diagnostic["sources_ready"]["consumer"]["code_sha256"] == "old-code"
    assert not (output / "publication-before.json").exists()
    assert not (output / "selected-family.json").exists()
    assert delivery.read(config["publication"]["prior"]) == {"families": {"protected": {"artifactDigest": "old"}}}


@pytest.mark.parametrize("reference", ["output", "dependency", "evidence"])
def test_independent_publication_refuses_joint_fec_promotion_before_network(release, tmp_path, monkeypatch, reference):
    config, *_ = release
    receipt = delivery.read(config["serving"]["receipt"])
    if reference == "output":
        receipt["output_membership"]["fec_table"]["family"] = "sample"
    elif reference == "dependency":
        receipt["views"]["fec_view"]["dependencies"]["table"] = {"family": "sample"}
    else:
        receipt["views"]["fec_view"]["evidence_generations"]["sample"] = "generation"
    config["serving"]["receipt"] = pin(Path(config["serving"]["receipt"]["path"]), receipt)
    monkeypatch.setattr(delivery, "Client", lambda *a: pytest.fail("Unsupported promotion reached client"))
    monkeypatch.setattr(delivery, "publication_owner", lambda: SimpleNamespace())
    with pytest.raises(ValueError, match="Joint FEC"):
        delivery.publish_data(config, tmp_path, {}, observe=False)


@pytest.mark.parametrize("selected", [True, False])
def test_publication_observation_uses_pinned_admission_owner_without_writes(release, tmp_path, monkeypatch, selected):
    import rulespec_artifacts
    from spicy_regs import generations

    config, *_ = release
    prior = delivery.read(config["publication"]["prior"])
    entry = {"artifactDigest": "sha256:new"}
    current = {"families": {**prior["families"], "sample": entry if selected else {"artifactDigest": "other"}}}
    calls = []

    def admit(directory, *, expected_pin):
        calls.append(directory)
        assert expected_pin.as_dict() == {"logicalId": "sample", "artifactDigest": "sha256:new"}
        return object()

    monkeypatch.setattr(generations, "admit_generation_for_publication", admit)
    monkeypatch.setattr(rulespec_artifacts, "iter_member_descriptors", lambda *a: [])
    monkeypatch.setattr(
        delivery,
        "publication_owner",
        lambda: SimpleNamespace(load_index=lambda url: current, generation_entry=lambda *a: entry),
    )
    output = tmp_path / "observed"
    output.mkdir()
    if selected:
        assert delivery.publish_data(config, output, prior, observe=True)["publication_write_requested"] is False
    else:
        with pytest.raises(ValueError, match="Candidate is not selected"):
            delivery.publish_data(config, output, prior, observe=True)
    assert len(calls) == 1


@pytest.mark.parametrize("drift", [False, True])
def test_fec_query_expectations_bind_selected_receipt(release, tmp_path, drift):
    config, *_ = release
    receipt = delivery.read(config["serving"]["receipt"])
    meaning = {
        "sql_sha256": "view",
        "evidence_generations": {"fec": "generation"},
        "population": "literal",
        "as_of": "2026-10-20",
    }
    receipt["views"]["fec_view"] = meaning
    config["readback"]["queries"].append(
        {
            "sql": "SELECT value FROM fec_view LIMIT 1",
            "columns": ["value"],
            "rows": [["1.10"]],
            "publication": {
                "fec_view": {
                    "release_compatibility": {
                        **meaning,
                        "status": "compatible",
                        "receipt_sha256": "sha256:" + config["serving"]["receipt"]["sha256"],
                    }
                }
            },
        }
    )
    if drift:
        config["readback"]["queries"][-1]["publication"]["fec_view"]["release_compatibility"]["sql_sha256"] = "changed"
        with pytest.raises(ValueError, match="FEC query must bind"):
            delivery.readback_inputs(config, receipt)
    else:
        assert delivery.readback_inputs(config, receipt) == delivery.read(config["readback"]["prior"])
