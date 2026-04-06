from __future__ import annotations

import csv
import io
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from wallet_analyzer.addresses import is_valid_solana_address
from wallet_analyzer.analysis import ProfitabilityAssessment, ProfitabilityThresholds, assess_wallet
from wallet_analyzer.birdeye import (
    BirdeyeAPIError,
    BirdeyeClient,
    TokenFundingSnapshot,
    TokenHolderSnapshot,
    TokenOverview,
    TokenTradeSnapshot,
)

ProgressEvent = dict[str, Any]
ProgressCallback = Callable[[ProgressEvent], None]


@dataclass(slots=True, frozen=True)
class TokenIntelOptions:
    holder_limit: int = 30
    trade_limit: int = 50
    early_buyer_limit: int = 20
    trader_limit: int = 20
    candidate_limit: int = 40
    profitability_duration: str = "90d"
    timeout: float = 20.0
    max_retries: int = 5
    min_request_interval: float = 0.35
    wallet_workers: int = 4
    funding_batch_size: int = 50
    profitability_thresholds: ProfitabilityThresholds = field(default_factory=ProfitabilityThresholds)


@dataclass(slots=True)
class CandidateWallet:
    wallet: str
    source_tags: set[str] = field(default_factory=set)
    holder_amount: float = 0.0
    holder_value_usd: float = 0.0
    holder_share_pct: float = 0.0
    trade_volume_usd: float = 0.0
    trade_count: int = 0
    early_rank: int | None = None
    first_trade_at: str | None = None
    funding_source: str | None = None
    funding_time: str | None = None
    funding_tx_hash: str | None = None
    funding_cluster_size: int = 0
    profitability: ProfitabilityAssessment | None = None
    alpha_score: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        profitability = self.profitability
        return {
            "wallet": self.wallet,
            "alpha_score": self.alpha_score,
            "source_tags": sorted(self.source_tags),
            "holder_amount": round(self.holder_amount, 6),
            "holder_value_usd": round(self.holder_value_usd, 2),
            "holder_share_pct": round(self.holder_share_pct, 4),
            "trade_volume_usd": round(self.trade_volume_usd, 2),
            "trade_count": self.trade_count,
            "early_rank": self.early_rank,
            "first_trade_at": self.first_trade_at,
            "funding_source": self.funding_source,
            "funding_time": self.funding_time,
            "funding_tx_hash": self.funding_tx_hash,
            "funding_cluster_size": self.funding_cluster_size,
            "wallet_status": profitability.status if profitability else None,
            "wallet_score": profitability.score if profitability else None,
            "wallet_total_profit_usd": round(profitability.summary.total_usd, 2) if profitability else None,
            "wallet_realized_profit_usd": round(profitability.summary.realized_profit_usd, 2) if profitability else None,
            "wallet_win_rate_pct": round(profitability.win_rate_pct, 2) if profitability else None,
            "notes": self.notes,
        }


@dataclass(slots=True)
class TokenIntelRun:
    token_address: str
    overview: TokenOverview
    report_payload: dict[str, Any]
    csv_text: str


def analyze_token_address(
    token_address: str,
    api_key: str,
    *,
    options: TokenIntelOptions | None = None,
    progress_callback: ProgressCallback | None = None,
) -> TokenIntelRun:
    token_address = token_address.strip()
    if not api_key.strip():
        raise ValueError("Birdeye API key is required.")
    if not is_valid_solana_address(token_address):
        raise ValueError("Enter a valid Solana token contract address.")

    resolved_options = options or TokenIntelOptions()
    _validate_options(resolved_options)

    client = BirdeyeClient(
        api_key=api_key,
        timeout=resolved_options.timeout,
        max_retries=resolved_options.max_retries,
        min_request_interval=resolved_options.min_request_interval,
    )

    _emit_progress(progress_callback, phase="prepare", completed=0, total=5, message="Loading token overview.")
    overview = client.fetch_token_overview(token_address)

    _emit_progress(progress_callback, phase="prepare", completed=1, total=5, message="Loading top holders.")
    holders = client.fetch_token_holders(token_address, limit=resolved_options.holder_limit)

    _emit_progress(progress_callback, phase="prepare", completed=2, total=5, message="Loading token trades to identify early buyers and active traders.")
    trades = client.fetch_token_trades(token_address, limit=resolved_options.trade_limit)

    candidates = _build_candidates(holders, trades, resolved_options)
    if not candidates:
        raise ValueError("Birdeye returned no candidate wallets for that token.")

    _emit_progress(progress_callback, phase="prepare", completed=3, total=5, message=f"Enriching {len(candidates)} candidate wallets with first funding sources.")
    funding_warning = _apply_funding_clusters(candidates, client, token_address, resolved_options)

    _emit_progress(progress_callback, phase="prepare", completed=4, total=5, message=f"Screening {len(candidates)} candidate wallets for profitability.")
    _apply_profitability(candidates, client, resolved_options, progress_callback)

    _score_candidates(candidates)
    ordered_candidates = sorted(candidates.values(), key=lambda item: (-item.alpha_score, item.early_rank or 999999, -item.trade_volume_usd))
    report_payload = _build_report_payload(token_address, overview, holders, trades, ordered_candidates, resolved_options, funding_warning)

    _emit_progress(
        progress_callback,
        phase="done",
        completed=len(ordered_candidates),
        total=len(ordered_candidates),
        message=f"Finished token intel for {overview.symbol or token_address}. Ranked {len(ordered_candidates)} candidate wallets.",
    )

    return TokenIntelRun(
        token_address=token_address,
        overview=overview,
        report_payload=report_payload,
        csv_text=_build_candidate_csv(ordered_candidates),
    )


def _validate_options(options: TokenIntelOptions) -> None:
    if options.holder_limit < 1:
        raise ValueError("holder_limit must be at least 1")
    if options.trade_limit < 1 or options.trade_limit > 50:
        raise ValueError("trade_limit must be between 1 and 50")
    if options.early_buyer_limit < 1:
        raise ValueError("early_buyer_limit must be at least 1")
    if options.trader_limit < 1:
        raise ValueError("trader_limit must be at least 1")
    if options.candidate_limit < 1:
        raise ValueError("candidate_limit must be at least 1")
    if options.wallet_workers < 1:
        raise ValueError("wallet_workers must be at least 1")
    if options.funding_batch_size < 1:
        raise ValueError("funding_batch_size must be at least 1")


def _build_candidates(
    holders: list[TokenHolderSnapshot],
    trades: list[TokenTradeSnapshot],
    options: TokenIntelOptions,
) -> dict[str, CandidateWallet]:
    candidates: dict[str, CandidateWallet] = {}

    def ensure(wallet: str) -> CandidateWallet:
        if wallet not in candidates:
            candidates[wallet] = CandidateWallet(wallet=wallet)
        return candidates[wallet]

    for holder in holders[: options.holder_limit]:
        if not holder.wallet:
            continue
        candidate = ensure(holder.wallet)
        candidate.source_tags.add("holder")
        candidate.holder_amount = max(candidate.holder_amount, holder.amount)
        candidate.holder_value_usd = max(candidate.holder_value_usd, holder.value_usd)
        candidate.holder_share_pct = max(candidate.holder_share_pct, holder.share_pct)

    early_count = 0
    trader_map: dict[str, dict[str, Any]] = defaultdict(lambda: {"volume": 0.0, "count": 0, "first_trade_at": None})
    for trade in trades:
        if not trade.wallet:
            continue
        trader = trader_map[trade.wallet]
        trader["volume"] += trade.volume_usd
        trader["count"] += 1
        if not trader["first_trade_at"] or (trade.block_time and trade.block_time < trader["first_trade_at"]):
            trader["first_trade_at"] = trade.block_time

        if trade.side == "buy" and early_count < options.early_buyer_limit:
            candidate = ensure(trade.wallet)
            if "early_buyer" not in candidate.source_tags:
                early_count += 1
                candidate.source_tags.add("early_buyer")
                candidate.early_rank = early_count
                candidate.first_trade_at = _isoformat(trade.block_time)

    ranked_traders = sorted(trader_map.items(), key=lambda item: (-item[1]["volume"], -item[1]["count"]))[: options.trader_limit]
    for wallet, stats in ranked_traders:
        candidate = ensure(wallet)
        candidate.source_tags.add("active_trader")
        candidate.trade_volume_usd = max(candidate.trade_volume_usd, float(stats["volume"]))
        candidate.trade_count = max(candidate.trade_count, int(stats["count"]))
        if not candidate.first_trade_at:
            candidate.first_trade_at = _isoformat(stats["first_trade_at"])

    ordered = sorted(
        candidates.values(),
        key=lambda item: (
            -item.holder_value_usd,
            item.early_rank or 999999,
            -item.trade_volume_usd,
        ),
    )[: options.candidate_limit]
    return {candidate.wallet: candidate for candidate in ordered}


def _apply_funding_clusters(
    candidates: dict[str, CandidateWallet],
    client: BirdeyeClient,
    token_address: str,
    options: TokenIntelOptions,
) -> str | None:
    wallets = list(candidates)
    funding_entries: list[TokenFundingSnapshot] = []
    try:
        for start in range(0, len(wallets), options.funding_batch_size):
            batch = wallets[start : start + options.funding_batch_size]
            funding_entries.extend(client.fetch_wallet_first_funded(batch, token_address=token_address))
    except BirdeyeAPIError as exc:
        warning = f"Funding cluster detection skipped because Birdeye denied access to first-funded data: {exc}"
        for candidate in candidates.values():
            candidate.notes.append("Funding cluster detection unavailable for this API key/package.")
        return warning

    by_wallet = {entry.wallet: entry for entry in funding_entries if entry.wallet}
    cluster_counter = Counter(entry.funded_by for entry in funding_entries if entry.funded_by)

    for wallet, candidate in candidates.items():
        funding = by_wallet.get(wallet)
        if not funding:
            continue
        candidate.funding_source = funding.funded_by
        candidate.funding_time = _isoformat(funding.block_time)
        candidate.funding_tx_hash = funding.tx_hash
        candidate.funding_cluster_size = cluster_counter.get(funding.funded_by, 0)
        if candidate.funding_cluster_size >= 2:
            candidate.notes.append(f"Shares first funding source with {candidate.funding_cluster_size - 1} other candidate wallet(s).")

    return None


def _apply_profitability(
    candidates: dict[str, CandidateWallet],
    client: BirdeyeClient,
    options: TokenIntelOptions,
    progress_callback: ProgressCallback | None = None,
) -> None:
    wallets = list(candidates)
    total = len(wallets)
    _emit_progress(progress_callback, phase="screening", completed=0, total=total, message=f"Scoring {total} candidate wallets by overall wallet profitability.")

    with ThreadPoolExecutor(max_workers=max(1, options.wallet_workers)) as executor:
        future_to_wallet = {
            executor.submit(client.fetch_summary, wallet, duration=options.profitability_duration): wallet
            for wallet in wallets
        }
        completed = 0
        for future in as_completed(future_to_wallet):
            completed += 1
            wallet = future_to_wallet[future]
            candidate = candidates[wallet]
            try:
                summary = future.result()
                candidate.profitability = assess_wallet(wallet, summary, options.profitability_thresholds)
                _emit_progress(
                    progress_callback,
                    phase="screening",
                    completed=completed,
                    total=total,
                    wallet=wallet,
                    outcome=candidate.profitability.status,
                    message=f"[{completed}/{total}] {wallet} -> {candidate.profitability.status}",
                )
            except BirdeyeAPIError as exc:
                candidate.notes.append(f"Wallet profitability fetch failed: {exc}")
                _emit_progress(
                    progress_callback,
                    phase="screening",
                    completed=completed,
                    total=total,
                    wallet=wallet,
                    outcome="request_failed",
                    message=f"[{completed}/{total}] {wallet} -> profitability lookup failed",
                )


def _score_candidates(candidates: dict[str, CandidateWallet]) -> None:
    for candidate in candidates.values():
        score = 0
        if "early_buyer" in candidate.source_tags:
            score += 26
            if candidate.early_rank is not None:
                score += max(0, 12 - min(candidate.early_rank, 12))
        if "holder" in candidate.source_tags:
            score += 12
            if candidate.holder_share_pct >= 1.0:
                score += 10
            elif candidate.holder_share_pct >= 0.25:
                score += 6
        if "active_trader" in candidate.source_tags:
            score += 14
            if candidate.trade_volume_usd >= 10000:
                score += 10
            elif candidate.trade_volume_usd >= 2500:
                score += 5
        if candidate.funding_cluster_size >= 2:
            score += min(14, candidate.funding_cluster_size * 3)
        if candidate.profitability:
            if candidate.profitability.status == "profitable":
                score += 30
            elif candidate.profitability.status == "borderline":
                score += 18
            elif candidate.profitability.status == "not_profitable":
                score += 4
            if candidate.profitability.summary.total_usd >= 1000:
                score += 8
            elif candidate.profitability.summary.total_usd > 0:
                score += 4
        else:
            score += 2

        candidate.alpha_score = int(max(0, min(100, round(score))))
        if not candidate.profitability:
            candidate.notes.append("Overall wallet profitability could not be verified from Birdeye.")
        elif candidate.profitability.status == "profitable":
            candidate.notes.append("Overall wallet profile clears the profitability thresholds.")
        if "early_buyer" in candidate.source_tags:
            candidate.notes.append("Appeared early in the sampled token trade flow.")


def _build_report_payload(
    token_address: str,
    overview: TokenOverview,
    holders: list[TokenHolderSnapshot],
    trades: list[TokenTradeSnapshot],
    candidates: list[CandidateWallet],
    options: TokenIntelOptions,
    funding_warning: str | None,
) -> dict[str, Any]:
    status_counts = Counter(
        candidate.profitability.status
        for candidate in candidates
        if candidate.profitability
    )
    funding_clusters = Counter(candidate.funding_source for candidate in candidates if candidate.funding_source)
    cluster_rows = []
    for funded_by, size in funding_clusters.most_common(10):
        if size < 2:
            continue
        cluster_rows.append(
            {
                "funding_source": funded_by,
                "wallet_count": size,
                "wallets": [candidate.wallet for candidate in candidates if candidate.funding_source == funded_by],
            }
        )

    analysis_notes = [
        "Early buyers are inferred from the earliest sampled token trades returned by Birdeye, not from full chain reconstruction.",
        "Cluster wallets are inferred from shared first-funding sources returned by Birdeye's wallet first-funded endpoint.",
        "Named KOL attribution is not included unless you later provide your own wallet label list.",
    ]
    if funding_warning:
        analysis_notes.append(funding_warning)

    return {
        "generated_at": _utc_now(),
        "token": {
            "address": token_address,
            "symbol": overview.symbol,
            "name": overview.name,
            "price_usd": overview.price,
            "market_cap": overview.market_cap,
            "liquidity_usd": overview.liquidity,
            "holders": overview.holder_count,
            "logo_uri": overview.logo_uri,
        },
        "settings": {
            "holder_limit": options.holder_limit,
            "trade_limit": options.trade_limit,
            "early_buyer_limit": options.early_buyer_limit,
            "trader_limit": options.trader_limit,
            "candidate_limit": options.candidate_limit,
            "profitability_duration": options.profitability_duration,
        },
        "summary": {
            "holders_sampled": len(holders),
            "trades_sampled": len(trades),
            "candidate_wallets": len(candidates),
            "profitable_wallets": status_counts.get("profitable", 0),
            "borderline_wallets": status_counts.get("borderline", 0),
            "clusters_found": len(cluster_rows),
        },
        "top_holders": [holder.to_dict() for holder in holders[:10]],
        "clusters": cluster_rows,
        "candidates": [candidate.to_dict() for candidate in candidates],
        "analysis_notes": analysis_notes,
    }


def _build_candidate_csv(candidates: list[CandidateWallet]) -> str:
    rows = [candidate.to_dict() for candidate in candidates]
    if not rows:
        rows = [{"wallet": "", "alpha_score": "", "source_tags": "", "notes": ""}]
    normalized = []
    for row in rows:
        normalized.append({
            **row,
            "source_tags": ", ".join(row.get("source_tags") or []),
            "notes": " | ".join(row.get("notes") or []),
        })
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(normalized[0].keys()))
    writer.writeheader()
    writer.writerows(normalized)
    return buffer.getvalue()


def _isoformat(block_time: int | None) -> str | None:
    if not block_time:
        return None
    return datetime.fromtimestamp(block_time, tz=timezone.utc).isoformat()


def _emit_progress(callback: ProgressCallback | None, **event: Any) -> None:
    if callback:
        callback(event)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
