from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from streaming.windows import WindowAggregator, compute_stats

T0 = datetime(2024, 5, 1, 12, 0, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


class TestComputeStats:
    def test_basic_metrics(self) -> None:
        stats = compute_stats(np.array([10.0, 20.0, 30.0, 40.0]))
        assert stats.count == 4
        assert stats.total == 100.0
        assert stats.avg == 25.0
        assert stats.min == 10.0
        assert stats.max == 40.0

    def test_p95_matches_numpy_linear_interpolation(self) -> None:
        values = np.arange(1, 101, dtype=float)
        assert compute_stats(values).p95 == pytest.approx(np.percentile(values, 95), abs=0.01)

    def test_empty_window_raises(self) -> None:
        with pytest.raises(ValueError):
            compute_stats(np.array([]))


class TestWindowAssignment:
    def test_tumbling_assigns_exactly_one_window(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10))
        assert agg.window_starts_for(at(7)) == [at(0)]
        assert agg.window_starts_for(at(10)) == [at(10)]  # el fin es exclusivo

    def test_sliding_assigns_size_over_step_windows(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=60), step=timedelta(seconds=10))
        starts = agg.window_starts_for(at(25))
        assert len(starts) == 6
        assert starts == [at(s) for s in (-30, -20, -10, 0, 10, 20)]
        assert all(s <= at(25) < s + timedelta(seconds=60) for s in starts)

    def test_window_type_is_inferred(self) -> None:
        assert WindowAggregator(size=timedelta(seconds=10)).window_type == "tumbling"
        assert WindowAggregator(size=timedelta(seconds=30), step=timedelta(seconds=10)).window_type == "sliding"

    @pytest.mark.parametrize(
        ("size", "step"),
        [(0, None), (10, 20), (30, 7)],
    )
    def test_invalid_configuration(self, size: int, step: int | None) -> None:
        with pytest.raises(ValueError):
            WindowAggregator(
                size=timedelta(seconds=size),
                step=timedelta(seconds=step) if step is not None else None,
            )

    def test_naive_datetimes_are_treated_as_utc(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10))
        assert agg.window_starts_for(datetime(2024, 5, 1, 12, 0, 7)) == [at(0)]


class TestTumblingAggregation:
    def test_window_is_emitted_only_after_watermark_passes_end(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10), allowed_lateness=timedelta(seconds=2))
        agg.add(at(1), 100.0)
        agg.add(at(5), 300.0)
        agg.add(at(11), 50.0)  # watermark = 9s, la ventana [0,10) sigue abierta
        assert agg.close_expired() == []

        agg.add(at(12), 50.0)  # watermark = 10s
        results = agg.close_expired()
        assert len(results) == 1
        result = results[0]
        assert (result.window_start, result.window_end) == (at(0), at(10))
        assert result.stats.count == 2
        assert result.stats.total == 400.0
        assert result.stats.avg == 200.0

    def test_groups_are_aggregated_separately_plus_total(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10))
        agg.add(at(1), 10.0, group="web")
        agg.add(at(2), 20.0, group="web")
        agg.add(at(3), 70.0, group="store")
        results = {r.group_key: r.stats for r in agg.close_expired(now=at(30))}

        assert set(results) == {"all", "web", "store"}
        assert results["all"].count == 3 and results["all"].total == 100.0
        assert results["web"].count == 2 and results["web"].total == 30.0
        assert results["store"].count == 1 and results["store"].total == 70.0

    def test_out_of_order_event_within_lateness_is_accepted(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10), allowed_lateness=timedelta(seconds=5))
        agg.add(at(12), 1.0)
        assert agg.add(at(8), 1.0) is True  # watermark = 7s, la ventana [0,10) sigue abierta
        agg.add(at(16), 1.0)
        first = agg.close_expired()[0]
        assert first.window_start == at(0)
        assert first.stats.count == 1

    def test_late_event_after_window_closed_is_dropped(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10))
        agg.add(at(1), 10.0)
        agg.add(at(15), 10.0)
        assert len(agg.close_expired()) == 1

        assert agg.add(at(3), 999.0) is False
        assert agg.late_events == 1
        # el evento tardío no reabre ni contamina ventanas
        assert all(r.stats.max < 999 for r in agg.flush())

    def test_processing_time_closes_idle_windows(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10), allowed_lateness=timedelta(seconds=2))
        agg.add(at(1), 5.0)
        assert agg.close_expired(now=at(11)) == []  # 11 - 2 = 9 < 10
        assert len(agg.close_expired(now=at(12))) == 1

    def test_each_window_is_emitted_once(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10))
        agg.add(at(1), 5.0)
        assert len(agg.close_expired(now=at(20))) == 1
        assert agg.close_expired(now=at(40)) == []

    def test_flush_emits_open_windows(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=10), include_total_group=False)
        agg.add(at(1), 5.0, group="web")
        agg.add(at(14), 5.0, group="web")
        results = agg.flush()
        assert [r.window_start for r in results] == [at(0), at(10)]
        assert agg.open_windows() == 0


class TestSlidingAggregation:
    def test_event_contributes_to_every_overlapping_window(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=30), step=timedelta(seconds=10))
        agg.add(at(5), 100.0)
        agg.add(at(15), 200.0)
        agg.add(at(25), 300.0)
        results = {r.window_start: r.stats for r in agg.close_expired(now=at(100))}

        # Ventanas que contienen al menos un evento: [-20,10) [-10,20) [0,30) [10,40) [20,50)
        assert results[at(0)].count == 3
        assert results[at(0)].total == 600.0
        assert results[at(-10)].total == 300.0  # eventos de 5s y 15s
        assert results[at(20)].total == 300.0  # solo el de 25s
        assert sum(s.count for s in results.values()) == 9  # cada evento en 3 ventanas

    def test_results_serialize_to_json_friendly_dict(self) -> None:
        agg = WindowAggregator(size=timedelta(seconds=20), step=timedelta(seconds=10))
        agg.add(at(1), 10.0, group="mobile")
        payload = agg.close_expired(now=at(60))[0].to_dict()
        assert payload["window_type"] == "sliding"
        assert payload["window_start"].endswith("+00:00")
        assert {"count", "total", "avg", "p95", "min", "max", "group_key"} <= payload.keys()
