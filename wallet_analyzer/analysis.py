from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from wallet_analyzer.birdeye import SummarySnapshot, TokenSnapshot


@dataclass(slots=True, frozen=True)
class ProfitabilityThresholds:
    min_total_trades: int = 15
    min_unique_tokens: int = 3
    min_total_invested_usd: float = 1000.0
    min_win_rate: float = 0.50
    min_realized_profit_usd: float = 250.0
    min_total_profit_usd: float = 500.0


@dataclass(slots=True)
class ProfitabilityAssessment:
    wallet: str
    status: str
    score: int
    status_reason: str
    summary: SummarySnapshot
    checks: dict[str, bool]
    top_tokens: list[TokenSnapshot] = field(default_factory=list)

    @property
    def win_rate_pct(self) -> float:
        return self.summary.win_rate * 100

    @property
    def estimated_total_roi_pct(self) -> float:
        if self.summary.total_invested <= 0:
            return 0.0
        return (self.summary.total_usd / self.summary.total_invested) * 100

    def top_tokens_label(self, limit: int = 3) -> str:
        if not self.top_tokens:
            return ""
        ranked = sorted(self.top_tokens, key=lambda token: token.total_usd, reverse=True)[:limit]
        parts = []
        for token in ranked:
            symbol = token.symbol or token.address[:6]
            parts.append(f"{symbol}:{token.total_usd:+.2f} USD")
        return "; ".join(parts)

    def to_flat_dict(self, top_token_count: int = 3) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "status": self.status,
            "score": self.score,
            "status_reason": self.status_reason,
            "unique_tokens": self.summary.unique_tokens,
            "total_buy": self.summary.total_buy,
            "total_sell": self.summary.total_sell,
            "total_trade": self.summary.total_trade,
            "total_win": self.summary.total_win,
            "total_loss": self.summary.total_loss,
            "win_rate_pct": round(self.win_rate_pct, 2),
            "total_invested_usd": round(self.summary.total_invested, 2),
            "total_sold_usd": round(self.summary.total_sold, 2),
            "current_value_usd": round(self.summary.current_value, 2),
            "realized_profit_usd": round(self.summary.realized_profit_usd, 2),
            "realized_profit_pct": round(self.summary.realized_profit_percent, 2),
            "unrealized_usd": round(self.summary.unrealized_usd, 2),
            "total_profit_usd": round(self.summary.total_usd, 2),
            "avg_profit_per_trade_usd": round(self.summary.avg_profit_per_trade_usd, 2),
            "estimated_total_roi_pct": round(self.estimated_total_roi_pct, 2),
            "passes_min_trades": self.checks["enough_trades"],
            "passes_min_unique_tokens": self.checks["enough_breadth"],
            "passes_min_capital": self.checks["enough_capital"],
            "passes_min_win_rate": self.checks["good_win_rate"],
            "passes_min_realized_profit": self.checks["profitable_realized"],
            "passes_min_total_profit": self.checks["profitable_total"],
            "top_tokens": self.top_tokens_label(limit=top_token_count),
        }


def assess_wallet(
    wallet: str,
    summary: SummarySnapshot,
    thresholds: ProfitabilityThresholds,
) -> ProfitabilityAssessment:
    checks = {
        "enough_trades": summary.total_trade >= thresholds.min_total_trades,
        "enough_breadth": summary.unique_tokens >= thresholds.min_unique_tokens,
        "enough_capital": summary.total_invested >= thresholds.min_total_invested_usd,
        "good_win_rate": summary.win_rate >= thresholds.min_win_rate,
        "profitable_realized": summary.realized_profit_usd >= thresholds.min_realized_profit_usd,
        "profitable_total": summary.total_usd >= thresholds.min_total_profit_usd,
    }

    score = _calculate_score(summary, thresholds)
    if not checks["enough_trades"] or not checks["enough_capital"]:
        status = "insufficient_history"
        reason = (
            f"Only {summary.total_trade} trades on ${summary.total_invested:.2f} invested; "
            "not enough history or deployed capital yet."
        )
        score = min(score, 49)
    elif all(checks.values()):
        status = "profitable"
        reason = (
            "Meets the configured thresholds for activity, capital, win rate, realized profit, "
            "and total profit."
        )
        score = max(score, 75)
    elif summary.total_usd > 0 or summary.realized_profit_usd > 0 or checks["good_win_rate"]:
        status = "borderline"
        reason = _build_borderline_reason(summary, checks)
        score = max(score, 50)
    else:
        status = "not_profitable"
        reason = "Negative or weak performance after clearing the minimum activity and capital filters."
        score = min(score, 59)

    return ProfitabilityAssessment(
        wallet=wallet,
        status=status,
        score=score,
        status_reason=reason,
        summary=summary,
        checks=checks,
    )


def _build_borderline_reason(summary: SummarySnapshot, checks: dict[str, bool]) -> str:
    positive_signals = []
    gaps = []

    if summary.total_usd > 0:
        positive_signals.append(f"positive total PnL (${summary.total_usd:.2f})")
    if summary.realized_profit_usd > 0:
        positive_signals.append(f"positive realized PnL (${summary.realized_profit_usd:.2f})")
    if checks["good_win_rate"]:
        positive_signals.append(f"healthy win rate ({summary.win_rate * 100:.1f}%)")

    if not checks["enough_breadth"]:
        gaps.append("too few unique tokens")
    if not checks["profitable_realized"]:
        gaps.append("realized profit below threshold")
    if not checks["profitable_total"]:
        gaps.append("total profit below threshold")

    positive_text = ", ".join(positive_signals) if positive_signals else "some positive signal"
    gap_text = ", ".join(gaps) if gaps else "one or more core checks"
    return f"{positive_text}, but still misses: {gap_text}."


def _calculate_score(summary: SummarySnapshot, thresholds: ProfitabilityThresholds) -> int:
    total_roi = _safe_ratio(summary.total_usd, summary.total_invested)
    realized_roi = _safe_ratio(summary.realized_profit_usd, summary.total_invested)

    components = (
        0.35 * _scale(total_roi, -0.25, 0.50),
        0.20 * _scale(realized_roi, -0.10, 0.25),
        0.20 * _scale(summary.win_rate, 0.30, 0.75),
        0.15 * _scale(summary.total_trade, thresholds.min_total_trades, thresholds.min_total_trades * 6),
        0.10 * _scale(summary.unique_tokens, 1, 10),
    )
    return round(sum(components) * 100)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _scale(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    if value <= low:
        return 0.0
    if value >= high:
        return 1.0
    return (value - low) / (high - low)
