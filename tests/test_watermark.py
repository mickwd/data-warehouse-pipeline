from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest

from batch.watermark import (
    ExtractionWindow,
    WatermarkState,
    advance_watermark,
    compute_extraction_window,
    ensure_utc,
)

NOW = datetime(2024, 6, 1, 15, 0, 0, tzinfo=UTC)
WM = datetime(2024, 6, 1, 14, 0, 0, tzinfo=UTC)


class TestComputeExtractionWindow:
    def test_applies_lookback_and_safety_lag(self) -> None:
        window = compute_extraction_window(
            WatermarkState("customers", WM),
            now=NOW,
            lookback=timedelta(minutes=10),
            safety_lag=timedelta(seconds=30),
        )
        assert window.lower == datetime(2024, 6, 1, 13, 50, tzinfo=UTC)
        assert window.upper == datetime(2024, 6, 1, 14, 59, 30, tzinfo=UTC)
        assert not window.is_empty

    def test_window_is_empty_when_watermark_is_ahead_of_upper(self) -> None:
        window = compute_extraction_window(
            WatermarkState("customers", NOW),
            now=NOW,
            lookback=timedelta(0),
            safety_lag=timedelta(minutes=1),
        )
        assert window.is_empty

    def test_converts_other_timezones_to_utc(self) -> None:
        buenos_aires = timezone(timedelta(hours=-3))
        window = compute_extraction_window(
            WatermarkState("x", datetime(2024, 6, 1, 11, 0, tzinfo=buenos_aires)),
            now=NOW,
            lookback=timedelta(0),
            safety_lag=timedelta(0),
        )
        assert window.lower == WM
        assert window.lower.tzinfo == UTC

    def test_rejects_negative_parameters(self) -> None:
        with pytest.raises(ValueError):
            compute_extraction_window(WatermarkState("x", WM), NOW, lookback=timedelta(minutes=-1))


class TestAdvanceWatermark:
    window = ExtractionWindow(lower=WM - timedelta(minutes=10), upper=NOW)

    def test_takes_max_extracted_value(self) -> None:
        extracted = pd.Series(pd.to_datetime(["2024-06-01T14:20:00Z", "2024-06-01T14:45:00Z", "2024-06-01T14:05:00Z"]))
        assert advance_watermark(WM, extracted, self.window) == datetime(2024, 6, 1, 14, 45, tzinfo=UTC)

    def test_never_moves_backwards(self) -> None:
        # Por el lookback pueden venir solo filas anteriores al watermark actual
        extracted = pd.Series(pd.to_datetime(["2024-06-01T13:55:00Z"]))
        assert advance_watermark(WM, extracted, self.window) == WM

    def test_keeps_watermark_when_nothing_was_extracted(self) -> None:
        assert advance_watermark(WM, None, self.window) == WM
        assert advance_watermark(WM, pd.Series([], dtype="datetime64[ns, UTC]"), self.window) == WM

    def test_is_capped_by_window_upper_bound(self) -> None:
        # Un reloj desfasado en origen no debe adelantar el watermark al futuro
        extracted = pd.Series(pd.to_datetime(["2024-06-01T16:00:00Z"]))
        assert advance_watermark(WM, extracted, self.window) == NOW

    def test_ignores_unparseable_values(self) -> None:
        extracted = pd.Series(["no-es-fecha", "2024-06-01T14:30:00Z", None])
        assert advance_watermark(WM, extracted, self.window) == datetime(2024, 6, 1, 14, 30, tzinfo=UTC)

    def test_naive_values_are_assumed_utc(self) -> None:
        assert ensure_utc(datetime(2024, 1, 1, 10, 0)) == datetime(2024, 1, 1, 10, 0, tzinfo=UTC)
