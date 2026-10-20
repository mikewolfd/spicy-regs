"""One qualified image push or a read-only registry association; no image build."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

from _mcp_readback import require

ACCOUNT = "174055408ff1560e60601c4d12c561c4"
PROFILE = "spicy-regs-civictechdc"
REGISTRY = f"registry.cloudflare.com/{ACCOUNT}/spicy-regs-reader"


def command(args, section, env, output, name, *, timeout=120):
    with (output / name).open("xb") as stdout, (output / (name + ".stderr")).open("xb") as stderr:
        result = subprocess.run(args, cwd=section["tools"], env=env, stdout=stdout, stderr=stderr, timeout=timeout)
    result.check_returncode()
    return (output / name).read_bytes()


@contextmanager
def docker_auth(env):
    normal = Path.home() / ".docker/config.json"
    settings = json.loads(normal.read_text()) if normal.exists() else {}
    with tempfile.TemporaryDirectory(prefix="spicy-regs-docker-auth-") as private:
        env["DOCKER_CONFIG"] = private
        path = Path(private) / "config.json"
        path.write_text(json.dumps({"auths": {}, "cliPluginsExtraDirs": settings.get("cliPluginsExtraDirs", [])}))
        path.chmod(0o600)
        plugins = Path.home() / ".docker/cli-plugins"
        if plugins.exists():
            (Path(private) / "cli-plugins").symlink_to(plugins, target_is_directory=True)
        yield


def image_operation(section, source, output, qualified, original, *, push):
    locks = [
        parts[0]
        for line in Path(section["build_inputs"]["path"]).read_text().splitlines()
        if len(parts := line.split()) == 2 and parts[1] == "uv.lock"
    ]
    require(len(locks) == 1, "Build inputs must bind exactly one uv.lock")
    docker = section["docker"]
    cli = [section["node"], "node_modules/wrangler/bin/wrangler.js"]
    env = {k: v for k, v in os.environ.items() if not k.startswith(("CF_", "CLOUDFLARE_", "DOCKER_"))}
    env.update(
        CLOUDFLARE_ACCOUNT_ID=ACCOUNT,
        DOCKER_HOST=section["docker_host"],
        WRANGLER_SEND_METRICS="false",
        WRANGLER_LOG_PATH="/dev/null",
        WRANGLER_LOG_SANITIZE="true",
        CI="1",
    )
    who = json.loads(command(cli + ["whoami", "--json"], section, env, output, "whoami.json"))
    require(
        who["loggedIn"]
        and any(a["id"] == ACCOUNT for a in who["accounts"])
        and {"containers:write", "cloudchamber:write"}.issubset(who["tokenPermissions"]),
        "Authenticated target/scopes differ",
    )
    tag = "spicy-regs-reader:" + source["commit"]
    current = json.loads(command([docker, "image", "inspect", tag], section, env, output, "local-image.json"))[0]
    for field in ("Id", "Architecture", "Os", "RootFS", "Config", "Descriptor"):
        require(current[field] == original[field], f"Local qualified image changed: {field}")
    digest = qualified["local_manifest_digest"]
    image = f"{REGISTRY}@{digest}"
    with docker_auth(env):
        if push:
            command(
                cli + ["containers", "push", tag, "--profile", PROFILE, "--path-to-docker", docker],
                section,
                env,
                output,
                "push.log",
                timeout=600,
            )
        else:
            # Existing Wrangler command, pull only. Capture in memory, never retain either credential or stderr.
            response = subprocess.run(
                cli + ["containers", "registries", "credentials", "--pull", "--json", "--profile", PROFILE],
                cwd=section["tools"],
                env=env,
                capture_output=True,
                timeout=60,
            )
            require(response.returncode == 0, "Registry pull authentication failed")
            credentials = json.loads(response.stdout)
            login = subprocess.run(
                [docker, "login", "--password-stdin", "--username", credentials["username"], "registry.cloudflare.com"],
                input=credentials["password"].encode(),
                env=env,
                capture_output=True,
                timeout=60,
            )
            require(login.returncode == 0, "Private Docker login failed")
        raw = command(
            [docker, "buildx", "imagetools", "inspect", "--raw", image],
            section,
            env,
            output,
            "registry-manifest.json",
        )
        require(
            "sha256:" + hashlib.sha256(raw).hexdigest() == digest and len(raw) == original["Descriptor"]["size"],
            "Registry manifest bytes differ",
        )
        manifest = json.loads(raw)
        require(
            manifest["config"]["digest"] == qualified["config_digest"]
            and len(manifest["layers"]) == len(original["RootFS"]["Layers"]),
            "Registry config/layers differ",
        )
        remote = json.loads(
            command(
                [docker, "buildx", "imagetools", "inspect", "--format", "{{json .Image}}", image],
                section,
                env,
                output,
                "registry-config.json",
            )
        )
        require(
            remote["architecture"] == original["Architecture"] == "amd64"
            and remote["os"] == original["Os"] == "linux"
            and remote["rootfs"]["diff_ids"] == original["RootFS"]["Layers"]
            and remote["config"] == original["Config"]
            and remote["created"] == original["Created"],
            "Remote platform/config/ordered layers differ",
        )
    pushed = json.loads(command([docker, "image", "inspect", tag], section, env, output, "pushed-image-inspect.json"))[
        0
    ]
    for field in ("Id", "Architecture", "Os", "RootFS", "Config", "Descriptor"):
        require(pushed[field] == original[field], f"Post-push qualified image changed: {field}")
    require(
        [ref for ref in pushed["RepoDigests"] if ref.startswith(REGISTRY + "@")] == [image],
        "Local image lacks the exact pushed registry digest",
    )
    association = {
        "status": "pass",
        "source_commit": source["commit"],
        "source_tree": source["tree"],
        "registry_image": image,
        "registry_manifest_digest": digest,
        "config_digest": manifest["config"]["digest"],
        "uv_lock_sha256": locks[0],
        "consumer": {**qualified["consumer"], "image_digest": digest},
        "qualification_sha256": section["qualification"]["sha256"],
        "push_requested": push,
    }
    (output / "registry-association.json").write_text(json.dumps(association, indent=2) + "\n")
    return association
