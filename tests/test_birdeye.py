import unittest

from wallet_analyzer.birdeye import TokenHolderSnapshot, TokenOverview, TokenTradeSnapshot


class BirdeyeParsingTests(unittest.TestCase):
    def test_token_overview_parses_nested_payload_fields(self) -> None:
        payload = {
            "tokenInfo": {
                "symbol": "ALPHA",
                "name": "Alpha Token",
                "logoURI": "https://example.com/logo.png",
            },
            "priceUsd": 0.42,
            "extensions": {
                "marketCap": 1250000,
                "liquidityUsd": 220000,
                "holderNumber": 1960,
            },
        }

        overview = TokenOverview.from_payload("Mint111111111111111111111111111111111111111", payload)

        self.assertEqual(overview.symbol, "ALPHA")
        self.assertEqual(overview.name, "Alpha Token")
        self.assertEqual(overview.price, 0.42)
        self.assertEqual(overview.market_cap, 1250000)
        self.assertEqual(overview.liquidity, 220000)
        self.assertEqual(overview.holder_count, 1960)

    def test_token_holder_snapshot_derives_value_from_nested_wallet_and_price(self) -> None:
        payload = {
            "owner": {"address": "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv"},
            "uiAmount": 1500,
            "priceUsd": 0.25,
            "ownership": 0.034,
        }

        holder = TokenHolderSnapshot.from_payload(payload)

        self.assertEqual(holder.wallet, "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv")
        self.assertEqual(holder.amount, 1500)
        self.assertEqual(holder.value_usd, 375)
        self.assertAlmostEqual(holder.share_pct, 3.4)

    def test_token_trade_snapshot_reads_nested_wallet_and_usd_amount(self) -> None:
        payload = {
            "type": "swapBuy",
            "owner": {"address": "7xKXtg2CW4f7mnu9vSo8p7uMsuK1B89Yv5Trs7X52X58"},
            "from": {"amountUsd": 812.55},
            "baseAmount": 120000,
            "blockUnixTime": 1700000000,
            "signature": "sig-123",
        }

        trade = TokenTradeSnapshot.from_payload(payload)

        self.assertEqual(trade.wallet, "7xKXtg2CW4f7mnu9vSo8p7uMsuK1B89Yv5Trs7X52X58")
        self.assertEqual(trade.side, "buy")
        self.assertEqual(trade.volume_usd, 812.55)
        self.assertEqual(trade.token_amount, 120000)
        self.assertEqual(trade.block_time, 1700000000)
        self.assertEqual(trade.tx_hash, "sig-123")


if __name__ == "__main__":
    unittest.main()
