import unittest

from wallet_analyzer.analysis import ProfitabilityThresholds, assess_wallet
from wallet_analyzer.birdeye import SummarySnapshot


THRESHOLDS = ProfitabilityThresholds(
    min_total_trades=15,
    min_unique_tokens=3,
    min_total_invested_usd=1000,
    min_win_rate=0.50,
    min_realized_profit_usd=250,
    min_total_profit_usd=500,
)


class AnalysisTests(unittest.TestCase):
    def test_assess_wallet_marks_profitable_wallets(self) -> None:
        summary = SummarySnapshot(
            unique_tokens=8,
            total_buy=30,
            total_sell=25,
            total_trade=55,
            total_win=10,
            total_loss=4,
            win_rate=0.68,
            total_invested=12000,
            total_sold=14000,
            current_value=2500,
            realized_profit_usd=1800,
            realized_profit_percent=15.0,
            unrealized_usd=400,
            total_usd=2200,
            avg_profit_per_trade_usd=40,
        )

        result = assess_wallet("wallet-1", summary, THRESHOLDS)

        self.assertEqual(result.status, "profitable")
        self.assertGreaterEqual(result.score, 75)

    def test_assess_wallet_marks_insufficient_history_wallets(self) -> None:
        summary = SummarySnapshot(
            unique_tokens=2,
            total_trade=6,
            total_invested=250,
            win_rate=1.0,
            realized_profit_usd=200,
            total_usd=200,
        )

        result = assess_wallet("wallet-2", summary, THRESHOLDS)

        self.assertEqual(result.status, "insufficient_history")
        self.assertLessEqual(result.score, 49)

    def test_assess_wallet_marks_borderline_wallets(self) -> None:
        summary = SummarySnapshot(
            unique_tokens=2,
            total_trade=30,
            total_invested=2500,
            win_rate=0.56,
            realized_profit_usd=100,
            total_usd=350,
            avg_profit_per_trade_usd=11,
        )

        result = assess_wallet("wallet-3", summary, THRESHOLDS)

        self.assertEqual(result.status, "borderline")
        self.assertGreaterEqual(result.score, 50)

    def test_assess_wallet_marks_not_profitable_wallets(self) -> None:
        summary = SummarySnapshot(
            unique_tokens=6,
            total_trade=40,
            total_invested=5000,
            win_rate=0.22,
            realized_profit_usd=-1200,
            total_usd=-1800,
            avg_profit_per_trade_usd=-45,
        )

        result = assess_wallet("wallet-4", summary, THRESHOLDS)

        self.assertEqual(result.status, "not_profitable")
        self.assertLessEqual(result.score, 59)


if __name__ == "__main__":
    unittest.main()
