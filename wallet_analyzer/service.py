from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any, Callable

from wallet_analyzer.addresses import LoadResult, LoadedWallet, load_wallets, load_wallets_from_content
from wallet_analyzer.analysis import ProfitabilityAssessment, ProfitabilityThresholds, assess_wallet
from wallet_analyzer.birdeye import BirdeyeAPIError, BirdeyeClient, WalletDetails
from wallet_analyzer.reporting import build_csv_text

ProgressEvent = dict[str, Any]
ProgressCallback = Callable[[ProgressEvent], None]


@dataclass(slots=True, frozen=True)
class ScreeningOptions:
    duration: str = "90d"
    details: str = "none"
    top_tokens: int = 3
    details_limit: int = 10
    workers: int = 4
    timeout: float = 20.0
    max_retries: int = 5
    min_request_interval: float = 0.85
    retry_passes: int = 2
    retry_backoff_seconds: float = 4.0
    thresholds: ProfitabilityThresholds = field(default_factory=ProfitabilityThresholds)


@dataclass(slots=True)
class ScreeningRun:
    source_name: str
    load_result: LoadResult
    results: list[ProfitabilityAssessment]
    request_errors: list[dict[str, Any]]
    details_map: dict[str, WalletDetails]
    report_payload: dict[str, Any]
    csv_text: str
    details_payload: dict[str, Any] | None = None


def screen_wallets_from_path(
    input_path: str | Path,
    api_key: str,
    *,
    address_column: str | None = None,
    options: ScreeningOptions | None = None,
    progress_callback: ProgressCallback | None = None,
) -> ScreeningRun:
    load_result = load_wallets(input_path, address_column=address_column)
    source_name = str(Path(input_path).resolve())
    return screen_loaded_wallets(
        load_result,
        api_key,
        source_name=source_name,
        options=options,
        progress_callback=progress_callback,
    )


def screen_wallets_from_content(
    content: str,
    filename: str,
    api_key: str,
    *,
    address_column: str | None = None,
    options: ScreeningOptions | None = None,
    progress_callback: ProgressCallback | None = None,
) -> ScreeningRun:
    load_result = load_wallets_from_content(content, filename=filename, address_column=address_column)
    return screen_loaded_wallets(
        load_result,
        api_key,
        source_name=filename,
        options=options,
        progress_callback=progress_callback,
    )


def screen_loaded_wallets(
    load_result: LoadResult,
    api_key: str,
    *,
    source_name: str,
    options: ScreeningOptions | None = None,
    progress_callback: ProgressCallback | None = None,
) -> ScreeningRun:
    if not api_key.strip():
        raise ValueError("Birdeye API key is required.")
    if not load_result.wallets:
        raise ValueError("No valid wallets were found in the input file.")

    resolved_options = options or ScreeningOptions()
    _validate_options(resolved_options)

    _emit_progress(
        progress_callback,
        phase="prepare",
        completed=0,
        total=len(load_result.wallets),
        message=f"Loaded {len(load_result.wallets)} valid wallets from {source_name}.",
    )

    client = BirdeyeClient(
        api_key=api_key,
        timeout=resolved_options.timeout,
        max_retries=resolved_options.max_retries,
        min_request_interval=resolved_options.min_request_interval,
    )

    assessments, request_errors = _screen_wallets(
        wallets=load_result.wallets,
        client=client,
        thresholds=resolved_options.thresholds,
        duration=resolved_options.duration,
        workers=resolved_options.workers,
        retry_passes=resolved_options.retry_passes,
        retry_backoff_seconds=resolved_options.retry_backoff_seconds,
        progress_callback=progress_callback,
    )

    details_map: dict[str, WalletDetails] = {}
    if resolved_options.details != "none" and assessments:
        targets = assessments
        if resolved_options.details == "profitable":
            targets = [result for result in assessments if result.status == "profitable"]
        details_map = _fetch_details(
            targets=targets,
            client=client,
            duration=resolved_options.duration,
            limit=resolved_options.details_limit,
            workers=min(resolved_options.workers, 4),
            progress_callback=progress_callback,
        )
        for assessment in assessments:
            details = details_map.get(assessment.wallet)
            if details:
                assessment.summary.current_value = details.summary.current_value
                assessment.top_tokens = details.tokens

    ordered_results = _sort_results(assessments)
    report_payload = _build_report_payload(
        source_name=source_name,
        load_result=load_result,
        options=resolved_options,
        results=ordered_results,
        request_errors=request_errors,
    )
    details_payload = None
    if details_map:
        details_payload = {
            "generated_at": _utc_now(),
            "wallets": {wallet: details.to_dict() for wallet, details in sorted(details_map.items())},
        }

    _emit_progress(
        progress_callback,
        phase="done",
        completed=len(ordered_results),
        total=len(load_result.wallets),
        message=(
            f"Finished screening {len(load_result.wallets)} wallets. "
            f"Profitable: {report_payload['summary']['profitable']}, "
            f"borderline: {report_payload['summary']['borderline']}, "
            f"request errors: {len(request_errors)}."
        ),
    )

    return ScreeningRun(
        source_name=source_name,
        load_result=load_result,
        results=ordered_results,
        request_errors=request_errors,
        details_map=details_map,
        report_payload=report_payload,
        csv_text=build_csv_text(ordered_results, top_token_count=resolved_options.top_tokens),
        details_payload=details_payload,
    )


def _validate_options(options: ScreeningOptions) -> None:
    if options.duration not in {"all", "90d", "30d", "7d", "24h"}:
        raise ValueError("duration must be one of: all, 90d, 30d, 7d, 24h")
    if options.details not in {"none", "profitable", "all"}:
        raise ValueError("details must be one of: none, profitable, all")
    if options.workers < 1:
        raise ValueError("workers must be at least 1")
    if options.details_limit < 1 or options.details_limit > 100:
        raise ValueError("details_limit must be between 1 and 100")
    if options.top_tokens < 1:
        raise ValueError("top_tokens must be at least 1")
    if options.max_retries < 0:
        raise ValueError("max_retries must be at least 0")
    if options.min_request_interval < 0:
        raise ValueError("min_request_interval must be at least 0")
    if options.retry_passes < 0:
        raise ValueError("retry_passes must be at least 0")
    if options.retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be at least 0")


def _screen_wallets(
    *,
    wallets: list[LoadedWallet],
    client: BirdeyeClient,
    thresholds: ProfitabilityThresholds,
    duration: str,
    workers: int,
    retry_passes: int,
    retry_backoff_seconds: float,
    progress_callback: ProgressCallback | None = None,
) -> tuple[list[ProfitabilityAssessment], list[dict[str, Any]]]:
    results: list[ProfitabilityAssessment] = []
    request_errors: list[dict[str, Any]] = []
    total = len(wallets)
    resolved_count = 0
    pending_wallets = list(wallets)
    total_attempt_rounds = retry_passes + 1

    _emit_progress(
        progress_callback,
        phase="screening",
        completed=0,
        total=total,
        message=(
            f"Starting wallet screening for {total} wallets. "
            f"Automatic retry passes: {retry_passes}."
        ),
    )

    for pass_index in range(total_attempt_rounds):
        if not pending_wallets:
            break

        current_workers = workers if pass_index == 0 else 1
        if pass_index > 0:
            _emit_progress(
                progress_callback,
                phase="screening",
                completed=resolved_count,
                total=total,
                outcome="retrying",
                message=(
                    f"Retry pass {pass_index + 1} of {total_attempt_rounds} "
                    f"for {len(pending_wallets)} wallet(s)."
                ),
            )

        next_pending: list[LoadedWallet] = []
        with ThreadPoolExecutor(max_workers=max(1, current_workers)) as executor:
            future_to_wallet = {
                executor.submit(_fetch_and_assess, wallet, client, duration, thresholds): wallet
                for wallet in pending_wallets
            }
            for future in as_completed(future_to_wallet):
                wallet = future_to_wallet[future]
                try:
                    assessment = future.result()
                except BirdeyeAPIError as exc:
                    if pass_index < retry_passes:
                        next_pending.append(wallet)
                        _emit_progress(
                            progress_callback,
                            phase="screening",
                            completed=resolved_count,
                            total=total,
                            wallet=wallet.wallet,
                            outcome="retry_scheduled",
                            message=(
                                f"{wallet.wallet} hit a request error. "
                                f"Scheduling retry {pass_index + 2} of {total_attempt_rounds}."
                            ),
                        )
                        continue

                    resolved_count += 1
                    request_errors.append(
                        {
                            "wallet": wallet.wallet,
                            "source_row": wallet.source_row,
                            "error": str(exc),
                        }
                    )
                    _emit_progress(
                        progress_callback,
                        phase="screening",
                        completed=resolved_count,
                        total=total,
                        wallet=wallet.wallet,
                        outcome="request_failed",
                        message=(
                            f"[{resolved_count}/{total}] {wallet.wallet} -> request failed "
                            f"after {pass_index + 1} attempt(s)."
                        ),
                    )
                    continue

                results.append(assessment)
                resolved_count += 1
                suffix = "" if pass_index == 0 else f" after retry {pass_index}"
                _emit_progress(
                    progress_callback,
                    phase="screening",
                    completed=resolved_count,
                    total=total,
                    wallet=wallet.wallet,
                    outcome=assessment.status,
                    message=f"[{resolved_count}/{total}] {wallet.wallet} -> {assessment.status}{suffix}",
                )

        pending_wallets = next_pending
        if pending_wallets and pass_index < retry_passes and retry_backoff_seconds > 0:
            wait_seconds = retry_backoff_seconds * (pass_index + 1)
            _emit_progress(
                progress_callback,
                phase="screening",
                completed=resolved_count,
                total=total,
                outcome="retry_wait",
                message=(
                    f"Waiting {wait_seconds:.1f}s before retrying {len(pending_wallets)} wallet(s)."
                ),
            )
            time.sleep(wait_seconds)

    return results, request_errors


def _fetch_details(
    *,
    targets: list[ProfitabilityAssessment],
    client: BirdeyeClient,
    duration: str,
    limit: int,
    workers: int,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, WalletDetails]:
    details_map: dict[str, WalletDetails] = {}
    if not targets:
        return details_map

    total = len(targets)
    _emit_progress(
        progress_callback,
        phase="details",
        completed=0,
        total=total,
        message=f"Fetching token details for {total} wallets.",
    )

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_to_wallet = {
            executor.submit(client.fetch_details, target.wallet, duration=duration, limit=limit): target.wallet
            for target in targets
        }
        completed = 0
        for future in as_completed(future_to_wallet):
            completed += 1
            wallet = future_to_wallet[future]
            try:
                details_map[wallet] = future.result()
                outcome = "details_loaded"
                message = f"[{completed}/{total}] Loaded token details for {wallet}."
            except BirdeyeAPIError:
                outcome = "details_failed"
                message = f"[{completed}/{total}] Token details request failed for {wallet}."
            _emit_progress(
                progress_callback,
                phase="details",
                completed=completed,
                total=total,
                wallet=wallet,
                outcome=outcome,
                message=message,
            )

    return details_map


def _fetch_and_assess(
    wallet: LoadedWallet,
    client: BirdeyeClient,
    duration: str,
    thresholds: ProfitabilityThresholds,
) -> ProfitabilityAssessment:
    summary = client.fetch_summary(wallet.wallet, duration=duration)
    return assess_wallet(wallet.wallet, summary, thresholds)


def _build_report_payload(
    *,
    source_name: str,
    load_result: LoadResult,
    options: ScreeningOptions,
    results: list[ProfitabilityAssessment],
    request_errors: list[dict[str, Any]],
) -> dict[str, Any]:
    status_counts = Counter(result.status for result in results)
    return {
        "generated_at": _utc_now(),
        "input": {
            "source": source_name,
            "valid_wallets": len(load_result.wallets),
            "invalid_rows": len(load_result.invalid_rows),
            "duplicates_skipped": load_result.duplicates_skipped,
            "address_column": load_result.address_column,
        },
        "settings": {
            "duration": options.duration,
            "thresholds": asdict(options.thresholds),
            "top_token_count": options.top_tokens,
            "details_mode": options.details,
            "details_limit": options.details_limit,
            "workers": options.workers,
            "max_retries": options.max_retries,
            "min_request_interval": options.min_request_interval,
            "retry_passes": options.retry_passes,
            "retry_backoff_seconds": options.retry_backoff_seconds,
        },
        "summary": {
            "screened_wallets": len(results),
            "profitable": status_counts.get("profitable", 0),
            "borderline": status_counts.get("borderline", 0),
            "not_profitable": status_counts.get("not_profitable", 0),
            "insufficient_history": status_counts.get("insufficient_history", 0),
            "request_errors": len(request_errors),
        },
        "wallets": [result.to_flat_dict(top_token_count=options.top_tokens) for result in results],
        "invalid_rows": [asdict(item) for item in load_result.invalid_rows],
        "request_errors": request_errors,
    }


def _sort_results(results: list[ProfitabilityAssessment]) -> list[ProfitabilityAssessment]:
    order = {
        "profitable": 0,
        "borderline": 1,
        "not_profitable": 2,
        "insufficient_history": 3,
    }
    return sorted(
        results,
        key=lambda result: (
            order.get(result.status, 99),
            -result.score,
            -result.summary.total_usd,
            -result.summary.realized_profit_usd,
        ),
    )


def _emit_progress(callback: ProgressCallback | None, **event: Any) -> None:
    if not callback:
        return
    callback(event)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
