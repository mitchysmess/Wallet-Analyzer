import unittest
from unittest.mock import patch

from wallet_analyzer.analysis import ProfitabilityThresholds
from wallet_analyzer.birdeye import SummarySnapshot, TokenFundingSnapshot, TokenHolderSnapshot, TokenOverview, TokenTradeSnapshot
from wallet_analyzer.token_intel import TokenIntelOptions, analyze_token_address

TOKEN_ADDRESS = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
WALLET_ONE = "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv"
WALLET_TWO = "7xKXtg2CW4f7mnu9vSo8p7uMsuK1B89Yv5Trs7X52X58"
WALLET_THREE = "8QfJ3vLhPZvtY2z9w4M8u6a5QH2nJHk2R4x5DwRj7f4a"
FUNDER = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"


class TokenIntelTests(unittest.TestCase):
    def test_analyze_token_address_builds_ranked_candidate_wallets(self) -> None:
        options = TokenIntelOptions(candidate_limit=10, wallet_workers=1)

        with patch("wallet_analyzer.token_intel.BirdeyeClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.fetch_token_overview.return_value = TokenOverview(
                address=TOKEN_ADDRESS,
                symbol="TEST",
                name="Test Token",
                price=0.42,
                market_cap=420000,
                liquidity=75000,
                holder_count=1234,
                logo_uri="",
            )
            mock_client.fetch_token_holders.return_value = [
                TokenHolderSnapshot(wallet=WALLET_ONE, amount=1000, value_usd=5000, share_pct=2.5),
                TokenHolderSnapshot(wallet=WALLET_TWO, amount=500, value_usd=2500, share_pct=1.0),
            ]
            mock_client.fetch_token_trades.return_value = [
                TokenTradeSnapshot(wallet=WALLET_ONE, side="buy", volume_usd=1200, block_time=1700000000),
                TokenTradeSnapshot(wallet=WALLET_THREE, side="buy", volume_usd=900, block_time=1700000100),
                TokenTradeSnapshot(wallet=WALLET_TWO, side="sell", volume_usd=3000, block_time=1700000200),
                TokenTradeSnapshot(wallet=WALLET_ONE, side="sell", volume_usd=2500, block_time=1700000300),
            ]
            mock_client.fetch_wallet_first_funded.return_value = [
                TokenFundingSnapshot(wallet=WALLET_ONE, funded_by=FUNDER, tx_hash="tx-1", block_time=1690000000),
                TokenFundingSnapshot(wallet=WALLET_TWO, funded_by=FUNDER, tx_hash="tx-2", block_time=1690000100),
                TokenFundingSnapshot(wallet=WALLET_THREE, funded_by=WALLET_THREE, tx_hash="tx-3", block_time=1690000200),
            ]

            def fake_fetch_summary(wallet, duration="90d"):
                if wallet == WALLET_ONE:
                    return SummarySnapshot(unique_tokens=5, total_trade=40, total_invested=5000, win_rate=0.7, realized_profit_usd=1600, total_usd=2200)
                if wallet == WALLET_TWO:
                    return SummarySnapshot(unique_tokens=3, total_trade=20, total_invested=1500, win_rate=0.55, realized_profit_usd=300, total_usd=700)
                return SummarySnapshot(unique_tokens=2, total_trade=10, total_invested=800, win_rate=0.4, realized_profit_usd=50, total_usd=100)

            mock_client.fetch_summary.side_effect = fake_fetch_summary

            run = analyze_token_address(TOKEN_ADDRESS, "test-key", options=options)

        self.assertEqual(run.report_payload["token"]["symbol"], "TEST")
        self.assertEqual(run.report_payload["summary"]["candidate_wallets"], 3)
        self.assertGreaterEqual(run.report_payload["summary"]["clusters_found"], 1)
        top_candidate = run.report_payload["candidates"][0]
        self.assertEqual(top_candidate["wallet"], WALLET_ONE)
        self.assertIn("holder", top_candidate["source_tags"])
        self.assertIn("early_buyer", top_candidate["source_tags"])
        self.assertEqual(top_candidate["wallet_status"], "profitable")
        self.assertIn(WALLET_ONE, run.csv_text)


if __name__ == "__main__":
    unittest.main()
