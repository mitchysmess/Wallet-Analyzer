from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {character: index for index, character in enumerate(BASE58_ALPHABET)}
AUTO_ADDRESS_COLUMNS = ("wallet", "address", "wallet_address", "owner", "solana_wallet")
JSON_ADDRESS_KEYS = (
    "trackedWalletAddress",
    "tracked_wallet_address",
    "wallet",
    "address",
    "wallet_address",
    "walletAddress",
    "owner",
    "publicKey",
    "pubkey",
    "solana_wallet",
)
JSON_LIST_KEYS = ("wallets", "items", "data", "results", "trackedWallets", "tracked_wallets")


@dataclass(slots=True, frozen=True)
class LoadedWallet:
    wallet: str
    source_row: int


@dataclass(slots=True, frozen=True)
class InvalidRow:
    source_row: int
    raw_value: str
    reason: str


@dataclass(slots=True)
class LoadResult:
    wallets: list[LoadedWallet]
    invalid_rows: list[InvalidRow]
    duplicates_skipped: int
    address_column: str | None = None


def decode_base58(value: str) -> bytes:
    number = 0
    for character in value:
        try:
            number = number * 58 + BASE58_INDEX[character]
        except KeyError as exc:
            raise ValueError(f"invalid base58 character: {character}") from exc

    decoded = bytearray()
    while number:
        number, remainder = divmod(number, 256)
        decoded.append(remainder)
    decoded.reverse()

    leading_zeroes = len(value) - len(value.lstrip("1"))
    return bytes([0] * leading_zeroes) + bytes(decoded)


def is_valid_solana_address(value: str) -> bool:
    candidate = value.strip()
    if not candidate or not 32 <= len(candidate) <= 44:
        return False

    try:
        decoded = decode_base58(candidate)
    except ValueError:
        return False
    return len(decoded) == 32


def load_wallets(path: str | Path, address_column: str | None = None) -> LoadResult:
    input_path = Path(path)
    content = input_path.read_text(encoding="utf-8")
    return load_wallets_from_content(content, filename=input_path.name, address_column=address_column)


def load_wallets_from_content(
    content: str,
    *,
    filename: str = "wallets.csv",
    address_column: str | None = None,
) -> LoadResult:
    suffix = Path(filename).suffix.lower()
    stripped = content.lstrip()
    if suffix == ".csv":
        return _load_csv_wallets_from_handle(io.StringIO(content), address_column=address_column)
    if suffix == ".json" or stripped.startswith("[") or stripped.startswith("{"):
        return _load_json_wallets(content, address_column=address_column)
    return _load_text_wallets_from_lines(content.splitlines())


def _load_text_wallets_from_lines(lines: list[str]) -> LoadResult:
    wallets: list[LoadedWallet] = []
    invalid_rows: list[InvalidRow] = []
    seen: set[str] = set()
    duplicates_skipped = 0

    for line_number, raw_line in enumerate(lines, start=1):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue
        if not is_valid_solana_address(value):
            invalid_rows.append(InvalidRow(source_row=line_number, raw_value=value, reason="not a valid Solana address"))
            continue
        if value in seen:
            duplicates_skipped += 1
            continue
        seen.add(value)
        wallets.append(LoadedWallet(wallet=value, source_row=line_number))

    return LoadResult(wallets=wallets, invalid_rows=invalid_rows, duplicates_skipped=duplicates_skipped)


def _load_csv_wallets_from_handle(handle: io.TextIOBase, address_column: str | None = None) -> LoadResult:
    wallets: list[LoadedWallet] = []
    invalid_rows: list[InvalidRow] = []
    duplicates_skipped = 0
    seen: set[str] = set()

    reader = csv.DictReader(handle)
    if not reader.fieldnames:
        raise ValueError("The CSV input does not contain a header row")

    column_name = _resolve_address_column(reader.fieldnames, explicit_column=address_column)
    for row_number, row in enumerate(reader, start=2):
        raw_value = (row.get(column_name) or "").strip()
        if not raw_value:
            invalid_rows.append(InvalidRow(source_row=row_number, raw_value="", reason=f"missing value in '{column_name}'"))
            continue
        if not is_valid_solana_address(raw_value):
            invalid_rows.append(InvalidRow(source_row=row_number, raw_value=raw_value, reason="not a valid Solana address"))
            continue
        if raw_value in seen:
            duplicates_skipped += 1
            continue
        seen.add(raw_value)
        wallets.append(LoadedWallet(wallet=raw_value, source_row=row_number))

    return LoadResult(
        wallets=wallets,
        invalid_rows=invalid_rows,
        duplicates_skipped=duplicates_skipped,
        address_column=column_name,
    )


def _load_json_wallets(content: str, address_column: str | None = None) -> LoadResult:
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError("The uploaded file looks like JSON, but it could not be parsed.") from exc

    if isinstance(payload, dict):
        for key in JSON_LIST_KEYS:
            nested = payload.get(key)
            if isinstance(nested, list):
                payload = nested
                break
        else:
            payload = [payload]

    if not isinstance(payload, list):
        raise ValueError("JSON input must be an array of wallet strings or wallet objects.")

    if all(isinstance(item, str) for item in payload):
        return _load_text_wallets_from_lines([str(item) for item in payload])

    if not all(isinstance(item, Mapping) for item in payload):
        raise ValueError("JSON arrays must contain only strings or only objects.")

    objects = [dict(item) for item in payload]
    address_key = _resolve_json_address_key(objects, explicit_column=address_column)
    return _load_object_wallets(objects, address_key)


def _load_object_wallets(items: list[dict[str, Any]], address_key: str) -> LoadResult:
    wallets: list[LoadedWallet] = []
    invalid_rows: list[InvalidRow] = []
    seen: set[str] = set()
    duplicates_skipped = 0

    for index, item in enumerate(items, start=1):
        raw_value = str(item.get(address_key) or "").strip()
        if not raw_value:
            invalid_rows.append(InvalidRow(source_row=index, raw_value="", reason=f"missing value in '{address_key}'"))
            continue
        if not is_valid_solana_address(raw_value):
            invalid_rows.append(InvalidRow(source_row=index, raw_value=raw_value, reason="not a valid Solana address"))
            continue
        if raw_value in seen:
            duplicates_skipped += 1
            continue
        seen.add(raw_value)
        wallets.append(LoadedWallet(wallet=raw_value, source_row=index))

    return LoadResult(
        wallets=wallets,
        invalid_rows=invalid_rows,
        duplicates_skipped=duplicates_skipped,
        address_column=address_key,
    )


def _resolve_address_column(fieldnames: list[str], explicit_column: str | None = None) -> str:
    if explicit_column:
        if explicit_column not in fieldnames:
            raise ValueError(f"address column '{explicit_column}' was not found in the CSV header")
        return explicit_column

    lowered = {name.strip().lower(): name for name in fieldnames}
    for candidate in AUTO_ADDRESS_COLUMNS:
        if candidate in lowered:
            return lowered[candidate]

    available = ", ".join(fieldnames)
    raise ValueError(
        "could not auto-detect the address column. "
        f"Use --address-column. Available columns: {available}"
    )


def _resolve_json_address_key(items: Iterable[dict[str, Any]], explicit_column: str | None = None) -> str:
    first_keys = set()
    for item in items:
        first_keys.update(item.keys())

    if explicit_column:
        if explicit_column not in first_keys:
            available = ", ".join(sorted(first_keys))
            raise ValueError(
                f"address field '{explicit_column}' was not found in the JSON objects. Available fields: {available}"
            )
        return explicit_column

    for key in JSON_ADDRESS_KEYS:
        if key in first_keys:
            return key

    available = ", ".join(sorted(first_keys))
    raise ValueError(
        "could not auto-detect the address field in the JSON objects. "
        f"Use --address-column. Available fields: {available}"
    )
