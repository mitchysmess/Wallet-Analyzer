import shutil
import unittest
from pathlib import Path

from wallet_analyzer.addresses import is_valid_solana_address, load_wallets, load_wallets_from_content


VALID_ADDRESS_ONE = "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv"
VALID_ADDRESS_TWO = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
TEST_TMP_ROOT = Path("tests") / "_tmp"
TEST_TMP_ROOT.mkdir(exist_ok=True)


class AddressLoadingTests(unittest.TestCase):
    def tearDown(self) -> None:
        shutil.rmtree(TEST_TMP_ROOT, ignore_errors=True)
        TEST_TMP_ROOT.mkdir(exist_ok=True)

    def test_is_valid_solana_address_accepts_base58_pubkeys(self) -> None:
        self.assertTrue(is_valid_solana_address(VALID_ADDRESS_ONE))
        self.assertTrue(is_valid_solana_address(VALID_ADDRESS_TWO))

    def test_is_valid_solana_address_rejects_bad_values(self) -> None:
        self.assertFalse(is_valid_solana_address(""))
        self.assertFalse(is_valid_solana_address("abc"))
        self.assertFalse(is_valid_solana_address("0OIl-not-base58"))

    def test_load_wallets_from_text_file_deduplicates_and_tracks_invalid_rows(self) -> None:
        input_file = TEST_TMP_ROOT / "wallets.txt"
        input_file.write_text(
            "\n".join(
                [
                    "# comment",
                    VALID_ADDRESS_ONE,
                    VALID_ADDRESS_ONE,
                    "not-a-wallet",
                    VALID_ADDRESS_TWO,
                ]
            ),
            encoding="utf-8",
        )

        result = load_wallets(input_file)

        self.assertEqual([item.wallet for item in result.wallets], [VALID_ADDRESS_ONE, VALID_ADDRESS_TWO])
        self.assertEqual(result.duplicates_skipped, 1)
        self.assertEqual(len(result.invalid_rows), 1)

    def test_load_wallets_from_csv_auto_detects_column(self) -> None:
        input_file = TEST_TMP_ROOT / "wallets.csv"
        input_file.write_text(
            "wallet,label\n"
            f"{VALID_ADDRESS_ONE},alpha\n"
            f"{VALID_ADDRESS_TWO},beta\n",
            encoding="utf-8",
        )

        result = load_wallets(input_file)

        self.assertEqual(result.address_column, "wallet")
        self.assertEqual([item.wallet for item in result.wallets], [VALID_ADDRESS_ONE, VALID_ADDRESS_TWO])

    def test_load_wallets_from_content_supports_csv_uploads(self) -> None:
        result = load_wallets_from_content(
            "wallet,label\n"
            f"{VALID_ADDRESS_ONE},alpha\n"
            "not-a-wallet,bad\n"
            f"{VALID_ADDRESS_TWO},beta\n",
            filename="upload.csv",
        )

        self.assertEqual([item.wallet for item in result.wallets], [VALID_ADDRESS_ONE, VALID_ADDRESS_TWO])
        self.assertEqual(result.address_column, "wallet")
        self.assertEqual(len(result.invalid_rows), 1)

    def test_load_wallets_from_content_supports_json_object_arrays(self) -> None:
        result = load_wallets_from_content(
            """
            [
              {"trackedWalletAddress": "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv", "name": "alpha"},
              {"trackedWalletAddress": "not-a-wallet", "name": "bad"},
              {"trackedWalletAddress": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "name": "beta"}
            ]
            """,
            filename="upload.txt",
        )

        self.assertEqual([item.wallet for item in result.wallets], [VALID_ADDRESS_ONE, VALID_ADDRESS_TWO])
        self.assertEqual(result.address_column, "trackedWalletAddress")
        self.assertEqual(len(result.invalid_rows), 1)


if __name__ == "__main__":
    unittest.main()
