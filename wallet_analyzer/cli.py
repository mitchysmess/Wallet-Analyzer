from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from wallet_analyzer.analysis import ProfitabilityThresholds
from wallet_analyzer.reporting import write_json_report
from wallet_analyzer.service import ScreeningOptions, screen_wallets_from_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wallet-analyzer",
        description="Bulk-screen Solana wallets and classify which ones look like profitable traders.",
    )
    parser.add_argument("input_path", help="Path to a .txt or .csv file containing Solana wallet addresses.")
    parser.add_argument("--api-key", default=os.getenv("BIRDEYE_API_KEY"), help="Birdeye API key. Defaults to BIRDEYE_API_KEY.")
    parser.add_argument(
        "--duration",
        choices=("all", "90d", "30d", "7d", "24h"),
        default="90d",
        help="PnL lookback window to analyze. Default: 90d.",
    )
    parser.add_argument("--address-column", help="CSV column containing the wallet address.")
    parser.add_argument("--output-dir", default="reports", help="Directory where the reports will be written.")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent API workers. Default: 4.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Per-request timeout in seconds. Default: 20.")
    parser.add_argument("--max-retries", type=int, default=5, help="Retries for transient API errors. Default: 5.")
    parser.add_argument(
        "--min-request-interval",
        type=float,
        default=0.85,
        help="Minimum spacing between Birdeye wallet requests in seconds. Default: 0.85.",
    )
    parser.add_argument(
        "--retry-passes",
        type=int,
        default=2,
        help="Extra screening passes for wallets that fail due to request errors. Default: 2.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=4.0,
        help="Base wait before retry passes; each pass multiplies it. Default: 4.0.",
    )
    parser.add_argument(
        "--details",
        choices=("none", "profitable", "all"),
        default="none",
        help="Optionally fetch token-level PnL details after screening. Default: none.",
    )
    parser.add_argument("--top-tokens", type=int, default=3, help="How many token winners to include in flat output. Default: 3.")
    parser.add_argument("--details-limit", type=int, default=10, help="Max token rows to request per wallet when details are enabled. Default: 10.")
    parser.add_argument("--min-total-trades", type=int, default=15)
    parser.add_argument("--min-unique-tokens", type=int, default=3)
    parser.add_argument("--min-total-invested-usd", type=float, default=1000.0)
    parser.add_argument("--min-win-rate", type=float, default=0.50, help="Fraction between 0 and 1. Default: 0.50.")
    parser.add_argument("--min-realized-profit-usd", type=float, default=250.0)
    parser.add_argument("--min-total-profit-usd", type=float, default=500.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.api_key:
        parser.error("Birdeye API key is required. Pass --api-key or set BIRDEYE_API_KEY.")

    thresholds = ProfitabilityThresholds(
        min_total_trades=args.min_total_trades,
        min_unique_tokens=args.min_unique_tokens,
        min_total_invested_usd=args.min_total_invested_usd,
        min_win_rate=args.min_win_rate,
        min_realized_profit_usd=args.min_realized_profit_usd,
        min_total_profit_usd=args.min_total_profit_usd,
    )
    options = ScreeningOptions(
        duration=args.duration,
        details=args.details,
        top_tokens=args.top_tokens,
        details_limit=args.details_limit,
        workers=args.workers,
        timeout=args.timeout,
        max_retries=args.max_retries,
        min_request_interval=args.min_request_interval,
        retry_passes=args.retry_passes,
        retry_backoff_seconds=args.retry_backoff_seconds,
        thresholds=thresholds,
    )

    try:
        run = screen_wallets_from_path(
            args.input_path,
            args.api_key,
            address_column=args.address_column,
            options=options,
            progress_callback=lambda event: print(event.get("message", ""), file=sys.stderr),
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "wallet_screen.csv"
    json_path = output_dir / "wallet_screen.json"
    details_path = output_dir / "wallet_details.json"

    csv_path.write_text(run.csv_text, encoding="utf-8")
    write_json_report(json_path, run.report_payload)
    if run.details_payload:
        write_json_report(details_path, run.details_payload)

    summary = run.report_payload["summary"]
    print(f"Screened {summary['screened_wallets']} wallet(s).")
    print(
        "Status counts: "
        f"profitable={summary['profitable']}, borderline={summary['borderline']}, "
        f"not_profitable={summary['not_profitable']}, insufficient_history={summary['insufficient_history']}"
    )
    print(f"CSV report:  {csv_path}")
    print(f"JSON report: {json_path}")
    if run.details_payload:
        print(f"Details:     {details_path}")
    if run.load_result.invalid_rows:
        print(f"Skipped {len(run.load_result.invalid_rows)} invalid row(s).")
    if run.load_result.duplicates_skipped:
        print(f"Skipped {run.load_result.duplicates_skipped} duplicate wallet(s).")
    if run.request_errors:
        print(f"{len(run.request_errors)} wallet request(s) failed. See JSON report for details.")

    if not run.results and run.request_errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
