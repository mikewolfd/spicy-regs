"""Pinned publication transport for regulatory runtime tests; real admission runs."""

import hashlib

from spicy_regs.sources import publication, r2
from tests.generation_fakes import Store


def install(monkeypatch):
    store = Store()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(r2, "require_credentials", lambda _: None)

    def current(_):
        raw = store.objects.get(publication.INDEX_V2_KEY)
        return publication.parse_index(raw) if raw else publication.empty_index()

    monkeypatch.setattr(publication, "current_index", current)

    def fetch(base, member, target, label):
        raw = store.objects.get(member.path)
        if raw is None:
            return False
        assert "sha256:" + hashlib.sha256(raw).hexdigest() == member.sha256
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        return True

    monkeypatch.setattr(publication, "fetch_member", fetch)
    return store
