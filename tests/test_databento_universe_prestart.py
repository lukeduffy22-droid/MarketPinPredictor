from datetime import date

import pytest

from tools.prepare_databento_universe_cache import (
    CACHE_ONLY_MODE,
    CURRENT_DAY_LABEL,
    PRIOR_SESSION_FALLBACK_LABEL,
    PROVIDER_ALLOWED_MODE,
    prepare_current_day_universe_cache,
)


class _FakeStreamer:
    def __init__(
        self,
        *,
        current_loads,
        stage_error=None,
        fallback=False,
        staged_prior=None,
    ):
        self._current_loads = iter(current_loads)
        self._stage_error = stage_error
        self._fallback = fallback
        self._staged_prior = staged_prior
        self.api_key = "secret-test-key"
        self.live_symbols = []
        self.stage_calls = 0
        self.prior_stage_calls = 0
        self.fallback_calls = 0
        self.primary_pair_admission_diagnostics = {}
        self.subscription_metadata = {
            "selected_universe_sha256": None,
            "universe_provenance": {"mode": "uninitialized", "is_fallback": False},
        }

    @staticmethod
    def _ready_market_plans(trading_date):
        plans = {}
        diagnostics = {}
        for market in ("SPX", "NDX"):
            admission = {
                "passes": True,
                "minimum_pair_count": 100,
                "complete_pair_count": 100,
            }
            diagnostics[market] = admission
            plans[market] = {
                "subscription_available": True,
                "primary_pair_admission": admission,
                "primary_expiration_authority": "primary_expiration",
                "primary_expiration_context_only": False,
                "primary_expiration_same_day_authority": True,
                "primary_expiration_selection_basis": (
                    "earliest_live_eligible_expiration"
                ),
                "selected_expirations": [
                    {
                        "role": "primary",
                        "expiration": trading_date.isoformat(),
                        "contracts": 200,
                        "selected_strike_pairs": 100,
                    }
                ],
            }
        return plans, diagnostics

    def _load_cached_universe(self, trading_date, *, ignore_refresh_flag=False):
        loaded = next(self._current_loads)
        if loaded:
            self.live_symbols = ["SPXW", "NDXP"]
            plans, diagnostics = self._ready_market_plans(trading_date)
            self.primary_pair_admission_diagnostics = diagnostics
            self.subscription_metadata = {
                "selected_universe_sha256": "selected-current",
                "markets": plans,
                "universe_provenance": {
                    "mode": "current_day_cache",
                    "trading_date": trading_date.isoformat(),
                    "source_date": trading_date.isoformat(),
                    "is_fallback": False,
                },
            }
        return loaded

    def stage_current_day_universe_cache(self, *, trading_date):
        self.stage_calls += 1
        if self._stage_error:
            raise self._stage_error
        return {
            "mode": "databento_discovery_staged",
            "trading_date": trading_date.isoformat(),
            "is_fallback": False,
        }

    def _load_prior_cached_universe(self, *, trading_date, reason):
        self.fallback_calls += 1
        if not self._fallback:
            return False
        self.live_symbols = ["SPXW-prior", "NDXP-prior"]
        plans, diagnostics = self._ready_market_plans(trading_date)
        self.primary_pair_admission_diagnostics = diagnostics
        self.subscription_metadata = {
            "selected_universe_sha256": "selected-prior",
            "markets": plans,
            "universe_provenance": {
                "mode": "prior_cache_filtered",
                "trading_date": trading_date.isoformat(),
                "source_date": "2026-08-25",
                "is_fallback": True,
                "reason": reason,
            },
        }
        return True

    def stage_prior_session_universe_cache(self, *, trading_date):
        self.prior_stage_calls += 1
        if self._staged_prior is None:
            raise RuntimeError("no provider-complete prior session")
        self._fallback = True
        return dict(self._staged_prior)


def _factory(fake):
    return lambda _symbols: fake


def test_prestart_reuses_valid_current_day_cache_without_provider_call():
    fake = _FakeStreamer(current_loads=[True])

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "ready"
    assert result["preparation_mode"] == PROVIDER_ALLOWED_MODE
    assert result["provenance_label"] == CURRENT_DAY_LABEL
    assert result["current_day_cache_ready"] is True
    assert result["provenance"]["is_fallback"] is False
    assert result["requested_symbols"] == ["SPX", "NDX"]
    assert set(result["market_primary_readiness"]) == {"SPX", "NDX"}
    assert all(
        market["subscription_available"] is True
        and market["admission_passes"] is True
        and market["primary_plan_count"] == 1
        and market["selected_strike_pairs"] == 100
        and market["complete_pair_count"] == 100
        and market["orb_reference_minimum_pair_count"] >= 5
        for market in result["market_primary_readiness"].values()
    )
    assert fake.stage_calls == 0
    assert fake.fallback_calls == 0


def test_prestart_evidence_exposes_missing_optional_primary_families():
    fake = _FakeStreamer(current_loads=[True])

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX", "VIX", "RUT"],
        trading_date=date(2026, 8, 26),
        streamer_factory=_factory(fake),
    )

    assert result["requested_symbols"] == ["SPX", "NDX", "VIX", "RUT"]
    for market in ("VIX", "RUT"):
        evidence = result["market_primary_readiness"][market]
        assert evidence["subscription_available"] is False
        assert evidence["admission_passes"] is False
        assert evidence["primary_plan_count"] == 0
        assert evidence["complete_pair_count"] == 0
        assert evidence["orb_reference_minimum_pair_count"] >= 5


def test_prestart_stages_and_revalidates_missing_current_day_cache():
    fake = _FakeStreamer(current_loads=[False, True])

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "staged"
    assert result["preparation_mode"] == PROVIDER_ALLOWED_MODE
    assert result["provenance_label"] == CURRENT_DAY_LABEL
    assert result["current_day_cache_ready"] is True
    assert fake.stage_calls == 1
    assert fake.fallback_calls == 0


def test_prestart_revalidates_staged_cache_when_refresh_flag_is_set(monkeypatch):
    fake = _FakeStreamer(current_loads=[False, True])
    monkeypatch.setenv("DATABENTO_REFRESH_CACHE", "1")

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "staged"
    assert fake.stage_calls == 1


def test_prestart_labels_bounded_prior_session_fallback():
    fake = _FakeStreamer(
        current_loads=[False],
        stage_error=RuntimeError("provider unavailable for secret-test-key"),
        fallback=True,
    )

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "fallback"
    assert result["preparation_mode"] == PROVIDER_ALLOWED_MODE
    assert result["provenance_label"] == PRIOR_SESSION_FALLBACK_LABEL
    assert result["current_day_cache_ready"] is False
    assert result["provenance"]["mode"] == "prior_cache_filtered"
    assert result["provenance"]["is_fallback"] is True
    assert "secret-test-key" not in result["warning"]


def test_prestart_fails_closed_without_current_or_prior_universe():
    fake = _FakeStreamer(
        current_loads=[False],
        stage_error=RuntimeError("provider unavailable"),
        fallback=False,
    )

    with pytest.raises(RuntimeError, match="No launch-safe Databento universe"):
        prepare_current_day_universe_cache(
            symbols=["SPX", "NDX"],
            trading_date=date(2026, 8, 26),
            streamer_factory=_factory(fake),
        )


def test_prestart_stages_provider_prior_day_only_as_explicit_fallback():
    fake = _FakeStreamer(
        current_loads=[False],
        stage_error=RuntimeError("current-day parent mappings unavailable"),
        fallback=False,
        staged_prior={
            "mode": "databento_prior_session_discovery_staged",
            "trading_date": "2026-08-26",
            "source_date": "2026-08-25",
            "source_path": "prior-cache.csv",
            "source_sha256": "provider-prior",
            "is_fallback": True,
            "applied_to_live_subscription": False,
        },
    )

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "fallback"
    assert result["provenance_label"] == PRIOR_SESSION_FALLBACK_LABEL
    assert result["current_day_cache_ready"] is False
    assert result["provenance"]["source_date"] == "2026-08-25"
    assert result["provenance"]["is_fallback"] is True
    assert result["staged_fallback_provenance"]["source_date"] == "2026-08-25"
    assert result["staged_fallback_provenance"]["is_fallback"] is True
    assert fake.prior_stage_calls == 1
    assert fake.fallback_calls == 2


def test_cache_only_reuses_current_cache_without_provider_discovery():
    fake = _FakeStreamer(current_loads=[True])

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        cache_only=True,
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "ready"
    assert result["preparation_mode"] == CACHE_ONLY_MODE
    assert result["provenance_label"] == CURRENT_DAY_LABEL
    assert fake.stage_calls == 0
    assert fake.prior_stage_calls == 0
    assert fake.fallback_calls == 0


def test_cache_only_uses_existing_prior_cache_without_provider_discovery():
    fake = _FakeStreamer(current_loads=[False], fallback=True)

    result = prepare_current_day_universe_cache(
        symbols=["SPX", "NDX"],
        trading_date=date(2026, 8, 26),
        cache_only=True,
        streamer_factory=_factory(fake),
    )

    assert result["status"] == "fallback"
    assert result["preparation_mode"] == CACHE_ONLY_MODE
    assert result["provenance_label"] == PRIOR_SESSION_FALLBACK_LABEL
    assert result["provenance"]["mode"] == "prior_cache_filtered"
    assert fake.stage_calls == 0
    assert fake.prior_stage_calls == 0
    assert fake.fallback_calls == 1


def test_cache_only_fails_closed_without_cache_and_never_discovers_provider():
    fake = _FakeStreamer(current_loads=[False], fallback=False)

    with pytest.raises(RuntimeError, match="provider discovery is disabled"):
        prepare_current_day_universe_cache(
            symbols=["SPX", "NDX"],
            trading_date=date(2026, 8, 26),
            cache_only=True,
            streamer_factory=_factory(fake),
        )

    assert fake.stage_calls == 0
    assert fake.prior_stage_calls == 0
    assert fake.fallback_calls == 1
