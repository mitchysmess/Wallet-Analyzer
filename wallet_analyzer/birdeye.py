from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Mapping
from urllib import error, parse, request

DEFAULT_BASE_URL = "https://public-api.birdeye.so"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 WalletAnalyzer/1.0"
)


class BirdeyeAPIError(RuntimeError):
    """Raised when a Birdeye request fails."""


@dataclass(slots=True)
class SummarySnapshot:
    unique_tokens: int = 0
    total_buy: int = 0
    total_sell: int = 0
    total_trade: int = 0
    total_win: int = 0
    total_loss: int = 0
    win_rate: float = 0.0
    total_invested: float = 0.0
    total_sold: float = 0.0
    current_value: float = 0.0
    realized_profit_usd: float = 0.0
    realized_profit_percent: float = 0.0
    unrealized_usd: float = 0.0
    total_usd: float = 0.0
    avg_profit_per_trade_usd: float = 0.0

    @classmethod
    def from_summary_payload(cls, payload: Mapping[str, Any] | None) -> "SummarySnapshot":
        payload = payload or {}
        counts = payload.get("counts") or {}
        cashflow = payload.get("cashflow_usd") or {}
        pnl = payload.get("pnl") or {}
        return cls(
            unique_tokens=_to_int(payload.get("unique_tokens")),
            total_buy=_to_int(counts.get("total_buy")),
            total_sell=_to_int(counts.get("total_sell")),
            total_trade=_to_int(counts.get("total_trade")),
            total_win=_to_int(counts.get("total_win")),
            total_loss=_to_int(counts.get("total_loss")),
            win_rate=_to_float(counts.get("win_rate")),
            total_invested=_to_float(cashflow.get("total_invested")),
            total_sold=_to_float(cashflow.get("total_sold")),
            current_value=_to_float(cashflow.get("current_value")),
            realized_profit_usd=_to_float(pnl.get("realized_profit_usd")),
            realized_profit_percent=_to_float(pnl.get("realized_profit_percent")),
            unrealized_usd=_to_float(pnl.get("unrealized_usd")),
            total_usd=_to_float(pnl.get("total_usd")),
            avg_profit_per_trade_usd=_to_float(pnl.get("avg_profit_per_trade_usd")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TokenSnapshot:
    address: str
    symbol: str
    total_trade: int
    realized_profit_usd: float
    unrealized_usd: float
    total_usd: float
    current_value: float

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "TokenSnapshot":
        counts = payload.get("counts") or {}
        cashflow = payload.get("cashflow_usd") or {}
        pnl = payload.get("pnl") or {}
        return cls(
            address=str(payload.get("address") or ""),
            symbol=str(payload.get("symbol") or ""),
            total_trade=_to_int(counts.get("total_trade")),
            realized_profit_usd=_to_float(pnl.get("realized_profit_usd")),
            unrealized_usd=_to_float(pnl.get("unrealized_usd")),
            total_usd=_to_float(pnl.get("total_usd")),
            current_value=_to_float(cashflow.get("current_value")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WalletDetails:
    wallet: str
    meta: dict[str, Any]
    summary: SummarySnapshot
    tokens: list[TokenSnapshot]

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "meta": self.meta,
            "summary": self.summary.to_dict(),
            "tokens": [token.to_dict() for token in self.tokens],
        }


@dataclass(slots=True)
class TokenOverview:
    address: str
    symbol: str = ""
    name: str = ""
    price: float = 0.0
    market_cap: float = 0.0
    liquidity: float = 0.0
    holder_count: int = 0
    logo_uri: str = ""

    @classmethod
    def from_payload(cls, address: str, payload: Mapping[str, Any] | None) -> "TokenOverview":
        payload = payload or {}
        return cls(
            address=address,
            symbol=str(payload.get("symbol") or payload.get("ticker") or ""),
            name=str(payload.get("name") or ""),
            price=_to_float(payload.get("price") or payload.get("price_usd") or payload.get("value")),
            market_cap=_to_float(payload.get("market_cap") or payload.get("mc") or payload.get("marketcap")),
            liquidity=_to_float(payload.get("liquidity") or payload.get("liquidity_usd")),
            holder_count=_to_int(payload.get("holder") or payload.get("holders") or payload.get("holder_count")),
            logo_uri=str(payload.get("logoURI") or payload.get("logo_uri") or payload.get("logo") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TokenHolderSnapshot:
    wallet: str
    amount: float = 0.0
    value_usd: float = 0.0
    share_pct: float = 0.0

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "TokenHolderSnapshot":
        return cls(
            wallet=str(payload.get("owner") or payload.get("wallet") or payload.get("address") or ""),
            amount=_to_float(payload.get("amount") or payload.get("ui_amount") or payload.get("balance")),
            value_usd=_to_float(payload.get("value") or payload.get("value_usd") or payload.get("usd_value")),
            share_pct=_normalize_percentage(payload.get("percentage") or payload.get("share") or payload.get("ratio")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TokenTradeSnapshot:
    wallet: str
    side: str
    volume_usd: float = 0.0
    token_amount: float = 0.0
    block_time: int | None = None
    tx_hash: str = ""

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "TokenTradeSnapshot":
        side = str(payload.get("side") or payload.get("tx_type") or payload.get("trade_type") or "").lower()
        if side in {"swapbuy", "buy_swap"}:
            side = "buy"
        elif side in {"swapsell", "sell_swap"}:
            side = "sell"
        return cls(
            wallet=str(
                payload.get("owner")
                or payload.get("wallet")
                or payload.get("user")
                or payload.get("maker")
                or payload.get("trader")
                or payload.get("source_owner")
                or ""
            ),
            side=side,
            volume_usd=_to_float(payload.get("value") or payload.get("volume_usd") or payload.get("amount_usd") or payload.get("usd")),
            token_amount=_to_float(payload.get("amount") or payload.get("token_amount") or payload.get("ui_amount")),
            block_time=_to_optional_int(payload.get("blockUnixTime") or payload.get("block_time") or payload.get("unix_time")),
            tx_hash=str(payload.get("txHash") or payload.get("tx_hash") or payload.get("signature") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TokenFundingSnapshot:
    wallet: str
    funded_by: str = ""
    tx_hash: str = ""
    block_time: int | None = None
    token_address: str = ""

    @classmethod
    def from_payload(cls, wallet: str, payload: Mapping[str, Any]) -> "TokenFundingSnapshot":
        return cls(
            wallet=wallet,
            funded_by=str(payload.get("sender") or payload.get("funded_by") or payload.get("source") or payload.get("from") or ""),
            tx_hash=str(payload.get("tx_hash") or payload.get("txHash") or payload.get("signature") or ""),
            block_time=_to_optional_int(payload.get("block_time") or payload.get("blockUnixTime") or payload.get("unix_time")),
            token_address=str(payload.get("token_address") or payload.get("address") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BirdeyeClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        chain: str = "solana",
        timeout: float = 20.0,
        max_retries: int = 5,
        user_agent: str = DEFAULT_USER_AGENT,
        min_request_interval: float = 0.85,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.chain = chain
        self.timeout = timeout
        self.max_retries = max_retries
        self.user_agent = user_agent
        self.min_request_interval = max(0.0, float(min_request_interval))
        self._rate_limit_lock = threading.Lock()
        self._next_request_at = 0.0

    def fetch_summary(self, wallet: str, *, duration: str = "90d") -> SummarySnapshot:
        payload = self._request_json(
            "GET",
            "/wallet/v2/pnl/summary",
            query={"wallet": wallet, "duration": duration},
        )
        data = payload.get("data") or {}
        summary_payload = data.get("summary") or data
        return SummarySnapshot.from_summary_payload(summary_payload)

    def fetch_details(
        self,
        wallet: str,
        *,
        duration: str = "90d",
        limit: int = 10,
        offset: int = 0,
    ) -> WalletDetails:
        payload = self._request_json(
            "POST",
            "/wallet/v2/pnl/details",
            body={
                "wallet": wallet,
                "duration": duration,
                "sort_by": "last_trade",
                "sort_type": "desc",
                "limit": limit,
                "offset": offset,
            },
        )
        data = payload.get("data") or {}
        tokens = [TokenSnapshot.from_payload(item) for item in data.get("tokens") or []]
        return WalletDetails(
            wallet=wallet,
            meta=dict(data.get("meta") or {}),
            summary=SummarySnapshot.from_summary_payload(data.get("summary")),
            tokens=tokens,
        )

    def fetch_token_overview(self, token_address: str) -> TokenOverview:
        payload = self._request_json("GET", "/defi/token_overview", query={"address": token_address})
        data = payload.get("data") or {}
        return TokenOverview.from_payload(token_address, data)

    def fetch_token_holders(self, token_address: str, *, limit: int = 30, offset: int = 0) -> list[TokenHolderSnapshot]:
        payload = self._request_json(
            "GET",
            "/defi/v3/token/holder",
            query={"address": token_address, "limit": limit, "offset": offset},
        )
        data = payload.get("data") or {}
        items = data.get("items") or data.get("holders") or data.get("list") or data
        if isinstance(items, list):
            return [TokenHolderSnapshot.from_payload(item) for item in items if isinstance(item, Mapping)]
        return []

    def fetch_token_trades(self, token_address: str, *, limit: int = 200, offset: int = 0) -> list[TokenTradeSnapshot]:
        try:
            payload = self._request_json(
                "GET",
                "/defi/v3/token/txs",
                query={"address": token_address, "limit": limit, "offset": offset, "sort_type": "asc"},
            )
        except BirdeyeAPIError:
            payload = self._request_json(
                "GET",
                "/defi/txs/token",
                query={"address": token_address, "limit": limit, "offset": offset, "sort_type": "asc"},
            )
        data = payload.get("data") or {}
        items = data.get("items") or data.get("txs") or data.get("history") or data
        if isinstance(items, list):
            return [
                trade
                for trade in (TokenTradeSnapshot.from_payload(item) for item in items if isinstance(item, Mapping))
                if trade.wallet
            ]
        return []

    def fetch_wallet_first_funded(self, wallets: list[str], *, token_address: str | None = None) -> list[TokenFundingSnapshot]:
        if not wallets:
            return []
        body: dict[str, Any] = {"wallets": wallets}
        if token_address:
            body["token_address"] = token_address
        payload = self._request_json("POST", "/wallet/v2/tx/first-funded", body=body)
        data = payload.get("data") or {}
        rows: list[TokenFundingSnapshot] = []
        if isinstance(data, Mapping):
            for wallet, entry in data.items():
                if isinstance(entry, Mapping):
                    rows.append(TokenFundingSnapshot.from_payload(str(wallet), entry))
                elif isinstance(entry, list) and entry and isinstance(entry[0], Mapping):
                    rows.append(TokenFundingSnapshot.from_payload(str(wallet), entry[0]))
        elif isinstance(data, list):
            for entry in data:
                if not isinstance(entry, Mapping):
                    continue
                wallet = str(entry.get("wallet") or entry.get("address") or "")
                if wallet:
                    rows.append(TokenFundingSnapshot.from_payload(wallet, entry))
        return rows

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if query:
            encoded_query = parse.urlencode(
                {key: value for key, value in query.items() if value is not None},
                doseq=True,
            )
            url = f"{url}?{encoded_query}"

        payload = None
        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key,
            "x-chain": self.chain,
            "User-Agent": self.user_agent,
        }
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["content-type"] = "application/json"

        request_object = request.Request(url, data=payload, headers=headers, method=method)
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                self._wait_for_request_slot()
                with request.urlopen(request_object, timeout=self.timeout) as response:
                    return _parse_json_response(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                body_text = exc.read().decode("utf-8", errors="replace")
                message = _extract_error_message(body_text) or body_text or exc.reason
                last_error = exc
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt, exc.headers.get("Retry-After") if exc.headers else None))
                    continue
                raise BirdeyeAPIError(f"{method} {path} failed with HTTP {exc.code}: {message}") from exc
            except error.URLError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt))
                    continue
                break
            except json.JSONDecodeError as exc:
                raise BirdeyeAPIError(f"{method} {path} returned invalid JSON") from exc

        raise BirdeyeAPIError(f"{method} {path} failed: {last_error}") from last_error

    def _wait_for_request_slot(self) -> None:
        if self.min_request_interval <= 0:
            return

        while True:
            wait_seconds = 0.0
            with self._rate_limit_lock:
                now = time.monotonic()
                wait_seconds = self._next_request_at - now
                if wait_seconds <= 0:
                    self._next_request_at = now + self.min_request_interval
                    return
            time.sleep(min(wait_seconds, 0.25))

    def _retry_delay(self, attempt: int, retry_after: str | None = None) -> float:
        header_delay = _parse_retry_after_seconds(retry_after)
        exponential_delay = max(self.min_request_interval, 1.5 ** attempt)
        return max(header_delay, exponential_delay)


def _parse_json_response(text: str) -> dict[str, Any]:
    payload = json.loads(text)
    if isinstance(payload, dict) and payload.get("success") is False:
        message = payload.get("message") or "API returned success=false"
        raise BirdeyeAPIError(str(message))
    if not isinstance(payload, dict):
        raise BirdeyeAPIError("API returned a non-object JSON payload")
    return payload


def _extract_error_message(text: str) -> str | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text.strip() or None
    if isinstance(payload, dict):
        return str(payload.get("message") or payload.get("detail") or "").strip() or None
    return None


def _parse_retry_after_seconds(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 0.0


def _normalize_percentage(value: Any) -> float:
    number = _to_float(value)
    if number > 1:
        return number
    return number * 100 if 0 < number <= 1 else 0.0


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(float(value))


def _to_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(float(value))
