from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence
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
            unique_tokens=_to_int(_pick_first(payload, "unique_tokens", "uniqueTokens", "token_num")),
            total_buy=_to_int(_pick_first(counts, "total_buy", "totalBuy", "buy")),
            total_sell=_to_int(_pick_first(counts, "total_sell", "totalSell", "sell")),
            total_trade=_to_int(_pick_first(counts, "total_trade", "totalTrade", "trade", "trades")),
            total_win=_to_int(_pick_first(counts, "total_win", "totalWin", "win")),
            total_loss=_to_int(_pick_first(counts, "total_loss", "totalLoss", "loss")),
            win_rate=_to_float(_pick_first(counts, "win_rate", "winRate")),
            total_invested=_to_float(_pick_first(cashflow, "total_invested", "totalInvested", "invested")),
            total_sold=_to_float(_pick_first(cashflow, "total_sold", "totalSold", "sold")),
            current_value=_to_float(_pick_first(cashflow, "current_value", "currentValue", "holding_value")),
            realized_profit_usd=_to_float(_pick_first(pnl, "realized_profit_usd", "realizedProfitUsd", "realized")),
            realized_profit_percent=_to_float(_pick_first(pnl, "realized_profit_percent", "realizedProfitPercent", "realized_percent")),
            unrealized_usd=_to_float(_pick_first(pnl, "unrealized_usd", "unrealizedUsd", "unrealized")),
            total_usd=_to_float(_pick_first(pnl, "total_usd", "totalUsd", "pnl", "profit")),
            avg_profit_per_trade_usd=_to_float(_pick_first(pnl, "avg_profit_per_trade_usd", "avgProfitPerTradeUsd")),
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
            address=str(_pick_first(payload, "address", "token_address", "mint") or ""),
            symbol=str(_pick_first(payload, "symbol", "ticker") or ""),
            total_trade=_to_int(_pick_first(counts, "total_trade", "totalTrade", "trades")),
            realized_profit_usd=_to_float(_pick_first(pnl, "realized_profit_usd", "realizedProfitUsd", "realized")),
            unrealized_usd=_to_float(_pick_first(pnl, "unrealized_usd", "unrealizedUsd", "unrealized")),
            total_usd=_to_float(_pick_first(pnl, "total_usd", "totalUsd", "pnl", "profit")),
            current_value=_to_float(_pick_first(cashflow, "current_value", "currentValue", "holding_value")),
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
            symbol=str(_find_first(payload, "symbol", "ticker", "tokenSymbol") or ""),
            name=str(_find_first(payload, "name", "tokenName") or ""),
            price=_to_float(_find_first(payload, "price", "price_usd", "priceUsd", "value", "last_price")),
            market_cap=_to_float(_find_first(payload, "market_cap", "marketcap", "marketCap", "mc", "fdv")),
            liquidity=_to_float(_find_first(payload, "liquidity", "liquidity_usd", "liquidityUsd", "total_liquidity")),
            holder_count=_to_int(_find_first(payload, "holder_count", "holders", "holder", "holderNumber")),
            logo_uri=str(_find_first(payload, "logoURI", "logo_uri", "logo", "image_uri", "icon") or ""),
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
        wallet = _extract_wallet_like(payload, ["owner", "wallet", "address", "holder", "user"])
        amount = _to_float(_find_first(payload, "ui_amount", "uiAmount", "amount", "balance", "token_amount"))
        value_usd = _to_float(_find_first(payload, "value_usd", "valueUsd", "usd_value", "usdValue", "value", "amount_usd"))
        share_pct = _normalize_percentage(_find_first(payload, "percentage", "share", "ratio", "ownership"))
        if value_usd <= 0 and amount > 0:
            price = _to_float(_find_first(payload, "price_usd", "priceUsd", "price"))
            if price > 0:
                value_usd = amount * price
        return cls(wallet=wallet, amount=amount, value_usd=value_usd, share_pct=share_pct)

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
        side = str(_find_first(payload, "side", "tx_type", "trade_type", "type") or "").lower()
        if side in {"swapbuy", "buy_swap", "buying"}:
            side = "buy"
        elif side in {"swapsell", "sell_swap", "selling"}:
            side = "sell"

        wallet = _extract_wallet_like(
            payload,
            ["owner", "wallet", "user", "maker", "trader", "source_owner", "signer", "from_owner", "user_address"],
        )
        volume_usd = _to_float(_find_first(payload, "volume_usd", "volumeUsd", "amount_usd", "amountUsd", "usd", "value", "base_value"))
        if volume_usd <= 0:
            from_usd = _to_float(_find_first(payload.get("from") if isinstance(payload.get("from"), Mapping) else {}, "amount_usd", "amountUsd", "usd", "value"))
            to_usd = _to_float(_find_first(payload.get("to") if isinstance(payload.get("to"), Mapping) else {}, "amount_usd", "amountUsd", "usd", "value"))
            volume_usd = max(from_usd, to_usd)
        token_amount = _to_float(_find_first(payload, "amount", "token_amount", "tokenAmount", "ui_amount", "uiAmount", "base_amount", "baseAmount"))
        return cls(
            wallet=wallet,
            side=side,
            volume_usd=volume_usd,
            token_amount=token_amount,
            block_time=_to_optional_int(_find_first(payload, "blockUnixTime", "block_time", "unix_time", "timestamp")),
            tx_hash=str(_find_first(payload, "txHash", "tx_hash", "signature", "hash") or ""),
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
            funded_by=str(_find_first(payload, "sender", "funded_by", "source", "from") or ""),
            tx_hash=str(_find_first(payload, "tx_hash", "txHash", "signature", "hash") or ""),
            block_time=_to_optional_int(_find_first(payload, "block_time", "blockUnixTime", "unix_time", "timestamp")),
            token_address=str(_find_first(payload, "token_address", "address", "mint") or ""),
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
        payload = self._request_json("GET", "/wallet/v2/pnl/summary", query={"wallet": wallet, "duration": duration})
        data = payload.get("data") or {}
        summary_payload = data.get("summary") or data
        return SummarySnapshot.from_summary_payload(summary_payload)

    def fetch_details(self, wallet: str, *, duration: str = "90d", limit: int = 10, offset: int = 0) -> WalletDetails:
        payload = self._request_json(
            "POST",
            "/wallet/v2/pnl/details",
            body={
                "wallet": wallet,
                "duration": duration,
                "sort_by": "last_trade",
                "sort_type": "desc",
                "limit": _clean_int(limit, minimum=1, maximum=100),
                "offset": _clean_int(offset, minimum=0),
            },
        )
        data = payload.get("data") or {}
        tokens = [TokenSnapshot.from_payload(item) for item in _extract_item_list(data)]
        return WalletDetails(wallet=wallet, meta=dict(data.get("meta") or {}), summary=SummarySnapshot.from_summary_payload(data.get("summary")), tokens=tokens)

    def fetch_token_overview(self, token_address: str) -> TokenOverview:
        payload = self._request_json("GET", "/defi/token_overview", query={"address": token_address})
        data = payload.get("data") or {}
        return TokenOverview.from_payload(token_address, data)

    def fetch_token_holders(self, token_address: str, *, limit: int = 30, offset: int = 0) -> list[TokenHolderSnapshot]:
        payload = self._request_json(
            "GET",
            "/defi/v3/token/holder",
            query={"address": token_address, "limit": _clean_int(limit, minimum=1, maximum=100), "offset": _clean_int(offset, minimum=0)},
        )
        items = _extract_item_list(payload.get("data") or {})
        return [TokenHolderSnapshot.from_payload(item) for item in items if isinstance(item, Mapping)]

    def fetch_token_trades(self, token_address: str, *, limit: int = 50, offset: int = 0) -> list[TokenTradeSnapshot]:
        cleaned_limit = _clean_int(limit, minimum=1, maximum=50)
        cleaned_offset = _clean_int(offset, minimum=0)
        try:
            payload = self._request_json(
                "GET",
                "/defi/v3/token/txs",
                query={"address": token_address, "limit": cleaned_limit, "offset": cleaned_offset, "sort_type": "asc"},
            )
        except BirdeyeAPIError:
            payload = self._request_json(
                "GET",
                "/defi/txs/token",
                query={"address": token_address, "limit": cleaned_limit, "offset": cleaned_offset, "sort_type": "asc"},
            )
        items = _extract_item_list(payload.get("data") or {})
        return [trade for trade in (TokenTradeSnapshot.from_payload(item) for item in items if isinstance(item, Mapping)) if trade.wallet]

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
                if isinstance(entry, Mapping):
                    wallet = str(_pick_first(entry, "wallet", "address") or "")
                    if wallet:
                        rows.append(TokenFundingSnapshot.from_payload(wallet, entry))
        return rows

    def _request_json(self, method: str, path: str, *, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if query:
            encoded_query = parse.urlencode({key: _normalize_query_value(value) for key, value in query.items() if value is not None}, doseq=True)
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


def _extract_item_list(data: Any) -> list[Mapping[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, Mapping)]
    if isinstance(data, Mapping):
        for key in ("items", "holders", "list", "txs", "history", "transactions", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
    return []


def _normalize_percentage(value: Any) -> float:
    number = _to_float(value)
    if number > 1:
        return number
    return number * 100 if 0 < number <= 1 else 0.0


def _normalize_query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else format(value, "g")
    return str(value)


def _extract_wallet_like(payload: Mapping[str, Any], keys: Sequence[str]) -> str:
    direct = _find_first(payload, *keys)
    if isinstance(direct, str) and 32 <= len(direct.strip()) <= 44:
        return direct.strip()
    for nested_key in ("owner", "wallet", "user", "trader", "maker", "from", "to"):
        nested = payload.get(nested_key)
        if isinstance(nested, Mapping):
            nested_value = _find_first(nested, *keys, "address", "wallet", "owner", "user")
            if isinstance(nested_value, str) and 32 <= len(nested_value.strip()) <= 44:
                return nested_value.strip()
    return str(direct or "")


def _find_first(payload: Any, *keys: str) -> Any:
    keyset = {key.lower() for key in keys}
    for current_key, value in _walk_mapping(payload):
        if current_key.lower() in keyset and value not in (None, ""):
            return value
    return None


def _pick_first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def _walk_mapping(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            yield str(key), nested
            yield from _walk_mapping(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_mapping(item)


def _clean_int(value: Any, *, minimum: int | None = None, maximum: int | None = None) -> int:
    number = _to_int(value)
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


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

