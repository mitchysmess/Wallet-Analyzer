from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from wallet_analyzer.analysis import ProfitabilityAssessment


EMPTY_REPORT_ROW = {
    "wallet": "",
    "status": "",
    "score": "",
    "status_reason": "",
    "unique_tokens": "",
    "total_buy": "",
    "total_sell": "",
    "total_trade": "",
    "total_win": "",
    "total_loss": "",
    "win_rate_pct": "",
    "total_invested_usd": "",
    "total_sold_usd": "",
    "current_value_usd": "",
    "realized_profit_usd": "",
    "realized_profit_pct": "",
    "unrealized_usd": "",
    "total_profit_usd": "",
    "avg_profit_per_trade_usd": "",
    "estimated_total_roi_pct": "",
    "passes_min_trades": "",
    "passes_min_unique_tokens": "",
    "passes_min_capital": "",
    "passes_min_win_rate": "",
    "passes_min_realized_profit": "",
    "passes_min_total_profit": "",
    "top_tokens": "",
}


def flat_report_rows(results: list[ProfitabilityAssessment], *, top_token_count: int = 3) -> list[dict[str, object]]:
    rows = [result.to_flat_dict(top_token_count=top_token_count) for result in results]
    return rows or [EMPTY_REPORT_ROW]


def build_csv_text(results: list[ProfitabilityAssessment], *, top_token_count: int = 3) -> str:
    rows = flat_report_rows(results, top_token_count=top_token_count)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def write_csv_report(path: str | Path, results: list[ProfitabilityAssessment], *, top_token_count: int = 3) -> None:
    output_path = Path(path)
    output_path.write_text(build_csv_text(results, top_token_count=top_token_count), encoding="utf-8", newline="")


def write_json_report(path: str | Path, payload: dict[str, object]) -> None:
    output_path = Path(path)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
