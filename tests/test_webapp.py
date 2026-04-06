import json
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from urllib import request
from unittest.mock import patch

from wallet_analyzer.addresses import LoadResult, LoadedWallet
from wallet_analyzer.analysis import ProfitabilityAssessment
from wallet_analyzer.birdeye import SummarySnapshot
from wallet_analyzer.service import ScreeningRun
import wallet_analyzer.webapp as webapp

VALID_ADDRESS = "J9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv"


class WebAppTests(unittest.TestCase):
    def setUp(self) -> None:
        webapp.JOBS.clear()
        handler_class = type(
            "TestWalletAnalyzerWebHandler",
            (webapp.WalletAnalyzerWebHandler,),
            {"api_key": "test-api-key"},
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        webapp.JOBS.clear()

    def test_health_endpoint_reports_configured_api_key(self) -> None:
        status, payload = self._request_json("GET", "/api/health")

        self.assertEqual(status, 200)
        self.assertTrue(payload["success"])
        self.assertTrue(payload["has_default_api_key"])

    def test_analyze_endpoint_runs_background_job_until_complete(self) -> None:
        assessment = ProfitabilityAssessment(
            wallet=VALID_ADDRESS,
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
        report_payload = {
            "generated_at": "2026-04-06T00:00:00+00:00",
            "input": {
                "source": "tracked-wallets.txt",
                "valid_wallets": 1,
                "invalid_rows": 0,
                "duplicates_skipped": 0,
                "address_column": "trackedWalletAddress",
            },
            "settings": {
                "duration": "90d",
                "top_token_count": 3,
                "details_mode": "none",
                "details_limit": 10,
                "workers": 4,
            },
            "summary": {
                "screened_wallets": 1,
                "profitable": 1,
                "borderline": 0,
                "not_profitable": 0,
                "insufficient_history": 0,
                "request_errors": 0,
            },
            "wallets": [assessment.to_flat_dict()],
            "invalid_rows": [],
            "request_errors": [],
        }
        fake_run = ScreeningRun(
            source_name="tracked-wallets.txt",
            load_result=LoadResult(
                wallets=[LoadedWallet(wallet=VALID_ADDRESS, source_row=1)],
                invalid_rows=[],
                duplicates_skipped=0,
                address_column="trackedWalletAddress",
            ),
            results=[assessment],
            request_errors=[],
            details_map={},
            report_payload=report_payload,
            csv_text="wallet,status\nJ9L6cQfT8f4V1y3m8YJQ26kQW6hgMHqYjgJv1nP9Wcv,profitable\n",
            details_payload=None,
        )

        def fake_screen_wallets_from_content(content, filename, api_key, **kwargs):
            progress_callback = kwargs.get("progress_callback")
            if progress_callback:
                progress_callback(
                    {
                        "phase": "prepare",
                        "completed": 0,
                        "total": 1,
                        "message": "Loaded 1 valid wallet from tracked-wallets.txt.",
                    }
                )
                progress_callback(
                    {
                        "phase": "screening",
                        "completed": 1,
                        "total": 1,
                        "wallet": VALID_ADDRESS,
                        "outcome": "profitable",
                        "message": f"[1/1] {VALID_ADDRESS} -> profitable",
                    }
                )
                progress_callback(
                    {
                        "phase": "done",
                        "completed": 1,
                        "total": 1,
                        "message": "Finished screening 1 wallets. Profitable: 1, borderline: 0, request errors: 0.",
                    }
                )
            time.sleep(0.05)
            return fake_run

        with patch("wallet_analyzer.webapp.screen_wallets_from_content", side_effect=fake_screen_wallets_from_content):
            status, payload = self._request_json(
                "POST",
                "/api/analyze",
                {
                    "file_name": "tracked-wallets.txt",
                    "content": "[]",
                    "duration": "90d",
                    "details": "none",
                },
            )

            self.assertEqual(status, 202)
            self.assertTrue(payload["success"])
            self.assertEqual(payload["download_prefix"], "tracked-wallets")

            deadline = time.time() + 5
            job = None
            while time.time() < deadline:
                job_status, job_payload = self._request_json("GET", f"/api/jobs/{payload['job_id']}")
                self.assertEqual(job_status, 200)
                job = job_payload["job"]
                if job["status"] in {"succeeded", "failed"}:
                    break
                time.sleep(0.05)

        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "succeeded")
        self.assertEqual(job["progress"]["phase"], "done")
        self.assertEqual(job["progress"]["progress_percent"], 100)
        self.assertEqual(job["result"]["report"]["summary"]["profitable"], 1)
        self.assertEqual(job["result"]["report"]["wallets"][0]["wallet"], VALID_ADDRESS)

    def _request_json(self, method: str, path: str, payload: dict[str, object] | None = None) -> tuple[int, dict[str, object]]:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        with request.urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
