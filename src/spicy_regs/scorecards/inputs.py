"""Reuse exact locally retained publication members before fetching missing bytes."""

from hashlib import file_digest
from pathlib import Path


def matches_member(path, member):
    if member.sha256 is None or member.byte_size is None:
        raise ValueError("Scorecard input requires an immutable member pin")
    if not path.is_file() or path.stat().st_size != member.byte_size:
        return False
    with path.open("rb") as stream:
        return "sha256:" + file_digest(stream, "sha256").hexdigest() == member.sha256


def stage_member(member, target, *, local_inputs=(), fetch):
    """Link verified local bytes into owned input staging; never alter shared files.

    Search only explicit roots and exact member locators. A wrong generation in
    a cache is skipped; an already-staged corrupt input refuses. Consumers still
    perform their normal generation and subject/receipt admission.
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        if not matches_member(target, member):
            raise ValueError("Staged scorecard input differs from its immutable pin")
        return target
    for root in local_inputs:
        root = Path(root).resolve()
        for locator in dict.fromkeys((member.path, member.key)):
            relative = Path(locator)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Scorecard input locator leaves its retained root")
            candidate = root / relative
            if candidate.is_file() and matches_member(candidate, member):
                target.symlink_to(candidate.resolve())
                return target
    if not fetch(member, target) or not matches_member(target, member):
        raise ValueError("Scorecard input is unavailable or differs from its immutable pin")
    return target
