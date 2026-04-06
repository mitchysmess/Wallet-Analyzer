import unittest
from unittest.mock import patch

from wallet_analyzer.addresses import LoadedWallet
from wallet_analyzer.analysis import ProfitabilityAssessment, ProfitabilityThresholds
from wallet_analyzer.birdeye import BirdeyeAPIError, SummarySnapshot
import wallet_analyzer.service as service

VALID_ADDRESS = "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv"
THRESHOLDS = ProfitabilityThresholds()


class ServiceRetryTests(unittest.TestCase):
    def test_screen_wallets_retries_failed_wallets_before_marking_error(self) -> None:
        wallets = [LoadedWallet(wallet=VALID_ADDRESS, source_row=1)]
        events: list[dict[str, object]] = []
        call_count = {"value": 0}

        def fake_fetch_and_assess(wallet, client, duration, thresholds):
            call_count["value"] += 1
            if call_count["value"] == 1:
                raise BirdeyeAPIError("HTTP 429")
            return _profitable_assessment(wallet.wallet)

        with patch("wallet_analyzer.service._fetch_and_assess", side_effect=fake_fetch_and_assess):
            results, request_errors = service._screen_wallets(
                wallets=wallets,
                client=object(),
                thresholds=THRESHOLDS,
                duration="90d",
                workers=1,
                retry_passes=1,
                retry_backoff_seconds=0,
                progress_callback=events.append,
            )

        self.assertEqual(call_count["value"], 2)
        self.assertEqual(len(results), 1)
        self.assertEqual(request_errors, [])
        self.assertTrue(any(event.get("outcome") == "retry_scheduled" for event in events))
        self.assertEqual(results[0].wallet, VALID_ADDRESS)

    def test_screen_wallets_keeps_request_error_when_retries_are_exhausted(self) -> None:
        wallets = [LoadedWallet(wallet=VALID_ADDRESS, source_row=1)]

        with patch("wallet_analyzer.service._fetch_and_assess", side_effect=BirdeyeAPIError("HTTP 429")):
            results, request_errors = service._screen_wallets(
                wallets=wallets,
                client=object(),
                thresholds=THRESHOLDS,
                duration="90d",
                workers=1,
                retry_passes=0,
                retry_backoff_seconds=0,
                progress_callback=None,
            )

        self.assertEqual(results, [])
        self.assertEqual(len(request_errors), 1)
        self.assertEqual(request_errors[0]["wallet"], VALID_ADDRESS)


def _profitable_assessment(wallet: str) -> ProfitabilityAssessment:
    return ProfitabilityAssessment(
        wallet=wallet,
        status="profitable",
        score=88,
        status_reason="Meets all configured thresholds.",
        summary=SummarySnapshot(
            unique_tokens=5,
            total_trade=24,
            total_invested=4200,
            win_rate=0.67,
            realized_profit_usd=1600,
            total_usd=2100,
        ),
        checks={
            "enough_trades": True,
            "enough_breadth": True,
            "enough_capital": True,
            "good_win_rate": True,
            "profitable_realized": True,
            "profitable_total": True,
        },
    )


if __name__ == "__main__":
    unittest.main()
