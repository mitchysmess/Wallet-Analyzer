from __future__ import annotations

import argparse
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from wallet_analyzer.analysis import ProfitabilityThresholds
from wallet_analyzer.service import ScreeningOptions, screen_wallets_from_content

WEB_ROOT = Path(__file__).with_name("webui")
STATIC_FILES = {
    "/": (WEB_ROOT / "index.html", "text/html; charset=utf-8"),
    "/index.html": (WEB_ROOT / "index.html", "text/html; charset=utf-8"),
    "/styles.css": (WEB_ROOT / "styles.css", "text/css; charset=utf-8"),
    "/app.js": (WEB_ROOT / "app.js", "application/javascript; charset=utf-8"),
}
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


class WalletAnalyzerWebHandler(BaseHTTPRequestHandler):
    server_version = "WalletAnalyzerWeb/0.3"
    api_key: str = ""

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/health":
            self._send_json(
                HTTPStatus.OK,
                {
                    "success": True,
                    "has_default_api_key": bool(self.api_key or os.getenv("BIRDEYE_API_KEY")),
                },
            )
            return

        if path.startswith("/api/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            job = _get_job(job_id)
            if not job:
                self._send_json(HTTPStatus.NOT_FOUND, {"success": False, "error": "Job not found"})
                return
            self._send_json(HTTPStatus.OK, {"success": True, "job": job})
            return

        file_entry = STATIC_FILES.get(path)
        if not file_entry:
            self._send_json(HTTPStatus.NOT_FOUND, {"success": False, "error": "Not found"})
            return

        file_path, content_type = file_entry
        self._send_file(file_path, content_type)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path != "/api/analyze":
            self._send_json(HTTPStatus.NOT_FOUND, {"success": False, "error": "Not found"})
            return

        try:
            payload = self._read_json_body()
            api_key = str(payload.get("api_key") or self.api_key or os.getenv("BIRDEYE_API_KEY") or "").strip()
            if not api_key:
                raise ValueError("Birdeye API key is required. Set BIRDEYE_API_KEY or enter it in the UI.")
            content = str(payload.get("content") or "")
            if not content.strip():
                raise ValueError("Upload a CSV, TXT, or JSON file before starting the analysis.")
            file_name = str(payload.get("file_name") or "wallets.csv")
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"success": False, "error": str(exc)})
            return

        job_id = uuid.uuid4().hex
        download_prefix = _safe_download_prefix(file_name)
        _create_job(job_id, file_name, download_prefix)
        worker = threading.Thread(
            target=_run_analysis_job,
            args=(job_id, payload, api_key, download_prefix),
            daemon=True,
        )
        worker.start()

        self._send_json(
            HTTPStatus.ACCEPTED,
            {
                "success": True,
                "job_id": job_id,
                "download_prefix": download_prefix,
            },
        )

    def log_message(self, format: str, *args: object) -> None:
        return

    def _read_json_body(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        if not raw_body:
            raise ValueError("Request body is empty.")
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("Request body must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ValueError("Request body must be a JSON object.")
        return payload

    def _send_file(self, file_path: Path, content_type: str) -> None:
        content = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wallet-analyzer-web",
        description="Start a small local web UI for the Solana wallet analyzer.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind to. Default: 8765")
    parser.add_argument("--api-key", default=os.getenv("BIRDEYE_API_KEY"), help="Optional Birdeye API key override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler_class = type(
        "ConfiguredWalletAnalyzerWebHandler",
        (WalletAnalyzerWebHandler,),
        {"api_key": args.api_key or ""},
    )
    server = ThreadingHTTPServer((args.host, args.port), handler_class)
    print(f"Wallet Analyzer UI running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop the server.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _run_analysis_job(job_id: str, payload: dict[str, Any], api_key: str, download_prefix: str) -> None:
    _update_job(job_id, status="running")
    _update_job_progress(
        job_id,
        {
            "phase": "queue",
            "completed": 0,
            "total": 0,
            "message": "Job accepted. Starting analysis worker.",
        },
    )

    try:
        file_name = str(payload.get("file_name") or "wallets.csv")
        address_column = str(payload.get("address_column") or "").strip() or None
        thresholds = ProfitabilityThresholds(
            min_total_trades=int(payload.get("min_total_trades", 15)),
            min_unique_tokens=int(payload.get("min_unique_tokens", 3)),
            min_total_invested_usd=float(payload.get("min_total_invested_usd", 1000.0)),
            min_win_rate=float(payload.get("min_win_rate", 0.50)),
            min_realized_profit_usd=float(payload.get("min_realized_profit_usd", 250.0)),
            min_total_profit_usd=float(payload.get("min_total_profit_usd", 500.0)),
        )
        options = ScreeningOptions(
            duration=str(payload.get("duration") or "90d"),
            details=str(payload.get("details") or "none"),
            top_tokens=int(payload.get("top_tokens", 3)),
            details_limit=int(payload.get("details_limit", 10)),
            workers=int(payload.get("workers", 4)),
            timeout=float(payload.get("timeout", 20.0)),
            max_retries=int(payload.get("max_retries", 5)),
            min_request_interval=float(payload.get("min_request_interval", 0.85)),
            retry_passes=int(payload.get("retry_passes", 2)),
            retry_backoff_seconds=float(payload.get("retry_backoff_seconds", 4.0)),
            thresholds=thresholds,
        )
        run = screen_wallets_from_content(
            str(payload.get("content") or ""),
            file_name,
            api_key,
            address_column=address_column,
            options=options,
            progress_callback=lambda event: _update_job_progress(job_id, event),
        )
    except Exception as exc:
        _update_job(
            job_id,
            status="failed",
            error=str(exc),
        )
        _update_job_progress(
            job_id,
            {
                "phase": "failed",
                "completed": 0,
                "total": 0,
                "message": str(exc),
                "progress_percent": 100,
            },
        )
        return

    _update_job(
        job_id,
        status="succeeded",
        result={
            "success": True,
            "download_prefix": download_prefix,
            "report": run.report_payload,
            "csv_content": run.csv_text,
            "details": run.details_payload,
        },
    )


def _create_job(job_id: str, file_name: str, download_prefix: str) -> None:
    now = _utc_now()
    with JOBS_LOCK:
        JOBS[job_id] = {
            "id": job_id,
            "file_name": file_name,
            "download_prefix": download_prefix,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "progress": {
                "phase": "queue",
                "completed": 0,
                "total": 0,
                "message": "Queued analysis job.",
                "progress_percent": 0,
            },
            "result": None,
            "error": None,
        }


def _get_job(job_id: str) -> dict[str, Any] | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        return json.loads(json.dumps(job))


def _update_job(job_id: str, **changes: Any) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job.update(changes)
        job["updated_at"] = _utc_now()


def _update_job_progress(job_id: str, event: dict[str, Any]) -> None:
    progress = dict(event)
    progress["progress_percent"] = progress.get(
        "progress_percent",
        _progress_percent(progress.get("phase", "queue"), progress.get("completed", 0), progress.get("total", 0)),
    )
    _update_job(job_id, progress=progress)


def _progress_percent(phase: str, completed: int, total: int) -> int:
    if phase == "done":
        return 100
    if phase == "failed":
        return 100
    if phase == "prepare":
        return 4
    if total <= 0:
        return 0
    ratio = max(0.0, min(1.0, completed / total))
    if phase == "screening":
        return round(ratio * 90)
    if phase == "details":
        return round(90 + ratio * 8)
    return round(ratio * 100)


def _safe_download_prefix(file_name: str) -> str:
    stem = Path(file_name).stem or "wallet-screen"
    cleaned = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in stem)
    return cleaned.strip("-") or "wallet-screen"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
