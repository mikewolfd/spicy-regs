"""Concurrency, backpressure and ownership checks using real source-reader primitives."""

from concurrent.futures import ThreadPoolExecutor
from threading import Lock, get_ident
from time import sleep

from spicy_regs.sources.derived_text import DerivedCommentText
from spicy_regs.transforms.derived_text_pool import DerivedTextPool
from tests.test_derived_text import ACF, _FakeS3Resource, _store


def _records(n=20):
    return [{"agency_code": ACF[0], "docket_id": ACF[1], "comment_id": f"{ACF[1]}-0030", "n": i} for i in range(n)]


def test_comments_share_budget_and_listing_but_never_resources_across_threads():
    lock = Lock()
    main_thread = get_ident()
    resources = []
    active = peak = 0

    class Resource(_FakeS3Resource):
        owner = None

        def Object(self, name, key):  # noqa: N802
            nonlocal active, peak
            with lock:
                if self.owner is None:
                    self.owner = get_ident()
                assert self.owner == get_ident()
                active += 1
                peak = max(peak, active)
            try:
                sleep(0.005)  # fixture network latency; long enough to overlap deterministically
                return super().Object(name, key)
            finally:
                with lock:
                    active -= 1

    def factory():
        assert get_ident() == main_thread  # boto3's default session never races
        resource = Resource(_store())
        resources.append(resource)
        return resource

    expected = DerivedCommentText(_FakeS3Resource(_store())).fill_for(*ACF, f"{ACF[1]}-0030")
    with DerivedTextPool(factory, max_workers=4) as pool, ThreadPoolExecutor(max_workers=6) as agencies:
        streams = [agencies.submit(lambda: list(pool.map(_records()))) for _ in range(6)]
        for stream in streams:
            results = stream.result(timeout=10)
            assert [item.record["n"] for item in results] == list(range(20))
            assert all(item.fill == expected for item in results)
    assert 1 < peak <= 4
    assert sum(len(resource.listed) for resource in resources) == 1
    assert len({resource.owner for resource in resources}) == 4


def test_slow_consumer_and_early_close_keep_the_queue_bounded():
    produced = 0

    def source():
        nonlocal produced
        for record in _records(100):
            produced += 1
            yield record

    with DerivedTextPool(lambda: _FakeS3Resource(_store()), max_workers=2) as pool:
        stream = pool.map(source())
        assert next(stream).fill is not None
        assert produced == 4
        stream.close()
        # Cancelled/finished futures return their slots. A subsequent stream
        # must complete rather than deadlock behind an abandoned consumer.
        assert len(list(pool.map(_records(10)))) == 10
