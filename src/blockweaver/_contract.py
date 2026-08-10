"""Configuration, feature, range, and RPC value contracts."""

from __future__ import annotations

import os
import re
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

OutputFormat = Literal["parquet", "csv"]
Source = Literal["rpc"]
Value = int | str

_INT64_MAX = 2**63 - 1
_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*\Z")
_ENV = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_DATETIME = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2})(?::(\d{2})(?::(\d{2}))?)?(Z|[+-]\d{2}:\d{2})\Z")


class BlockweaverError(Exception):
    """Expected failure with a stable machine-readable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class Feature:
    name: str
    dtype: Literal["Int64", "UTF-8"]
    unit: str
    family: Literal["header", "fee_history"]
    rpc_field: str | None = None
    dependencies: tuple[str, ...] = ()
    percentile: int | None = None
    domain: str = ""

    def document(self, *, available: bool = True) -> dict[str, object]:
        return {
            "name": self.name,
            "type": self.dtype,
            "unit": self.unit,
            "source_support": ["rpc"],
            "acquisition_family": self.family,
            "domain_rule": self.domain,
            "hidden_dependencies": list(self.dependencies),
            "available": available,
        }


FEATURES = (
    Feature("timestamp", "Int64", "unix_second", "header", "timestamp", domain="nonnegative, nondecreasing"),
    Feature("block_hash", "UTF-8", "hex", "header", "hash", domain="lowercase 0x-prefixed 32-byte hash"),
    Feature("parent_hash", "UTF-8", "hex", "header", "parentHash", domain="lowercase 0x-prefixed 32-byte hash"),
    Feature("base_fee_per_gas", "Int64", "wei/gas", "header", "baseFeePerGas", domain="positive"),
    Feature("gas_used", "Int64", "gas", "header", "gasUsed", ("gasLimit",), domain="nonnegative and at most gas_limit"),
    Feature("gas_limit", "Int64", "gas", "header", "gasLimit", ("gasUsed",), domain="positive"),
    Feature("tx_count", "Int64", "transaction", "header", "transactions", domain="nonnegative"),
    Feature("effective_priority_fee_per_gas_p50", "Int64", "wei/gas", "fee_history", percentile=50, domain="nonnegative"),
    Feature("effective_priority_fee_per_gas_p90", "Int64", "wei/gas", "fee_history", percentile=90, domain="nonnegative"),
)
FEATURE_BY_NAME = {feature.name: feature for feature in FEATURES}


@dataclass(frozen=True, slots=True)
class Plan:
    features: tuple[Feature, ...]
    header_fields: tuple[str, ...]
    percentiles: tuple[int, ...]

    @property
    def columns(self) -> tuple[str, ...]:
        return ("block_number", *(feature.name for feature in self.features))

    @property
    def schema(self) -> dict[str, str]:
        return {"block_number": "Int64", **{feature.name: feature.dtype for feature in self.features}}

    def schema_document(self) -> list[dict[str, str]]:
        return [
            {"name": "block_number", "type": "Int64", "unit": "block"},
            *({"name": feature.name, "type": feature.dtype, "unit": feature.unit} for feature in self.features),
        ]

    def document(self) -> dict[str, object]:
        families: list[dict[str, object]] = [
            {
                "family": "header",
                "method": "eth_getBlockByNumber",
                "fields": list(self.header_fields),
            }
        ]
        if self.percentiles:
            families.append(
                {
                    "family": "fee_history",
                    "method": "eth_feeHistory",
                    "reward_percentiles": list(self.percentiles),
                }
            )
        return {"families": families}


def plan_features(names: list[str] | tuple[str, ...]) -> Plan:
    if len(names) != len(set(names)):
        raise BlockweaverError("FEATURE_INVALID", "Features must not contain duplicates")
    if "block_number" in names:
        raise BlockweaverError("FEATURE_INVALID", "block_number is always included and must not be selected")
    unknown = sorted(set(names) - FEATURE_BY_NAME.keys())
    if unknown:
        raise BlockweaverError("FEATURE_INVALID", f"Unknown feature: {unknown[0]}")
    selected = tuple(feature for feature in FEATURES if feature.name in names)
    fields = {feature.rpc_field for feature in selected if feature.rpc_field is not None}
    for feature in selected:
        fields.update(feature.dependencies)
    ordered_fields = tuple(
        field
        for field in ("number", "hash", "parentHash", "timestamp", "baseFeePerGas", "gasUsed", "gasLimit", "transactions")
        if field in {"number", "hash", "parentHash", "timestamp"} or field in fields
    )
    percentiles = tuple(feature.percentile for feature in selected if feature.percentile is not None)
    return Plan(selected, ordered_fields, percentiles)


@dataclass(frozen=True, slots=True)
class ProviderSpec:
    name: str
    url: str | None
    url_env: str | None
    batch_size: int
    concurrency: int
    timeout: float


@dataclass(frozen=True, slots=True)
class Provider:
    name: str
    url: str
    batch_size: int
    concurrency: int
    timeout: float

    def __post_init__(self) -> None:
        _name(self.name, "provider name")
        _validate_url(self.url)
        _positive_int(self.batch_size, "batch_size")
        _positive_int(self.concurrency, "concurrency")
        _positive_number(self.timeout, "timeout")


@dataclass(frozen=True, slots=True)
class Chain:
    name: str
    chain_id: int
    finality_tag: Literal["finalized", "safe"]
    provider: str | None
    verifier: str | None


@dataclass(frozen=True, slots=True)
class Defaults:
    chain: str
    source: Source
    provider: str
    verifier: str
    output_root: Path
    output_format: OutputFormat
    features: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Config:
    path: Path
    defaults: Defaults
    chains: dict[str, Chain]
    providers: dict[str, ProviderSpec]

    def chain(self, name: str | None) -> Chain:
        selected = name or self.defaults.chain
        try:
            return self.chains[selected]
        except KeyError:
            raise BlockweaverError("CONFIG_INVALID", f"Unknown chain profile: {selected}") from None

    def provider(
        self,
        name: str,
        *,
        url: str | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        timeout: float | None = None,
    ) -> Provider:
        try:
            spec = self.providers[name]
        except KeyError:
            raise BlockweaverError("CONFIG_INVALID", f"Unknown RPC provider profile: {name}") from None
        resolved_url = url or spec.url
        if resolved_url is None:
            assert spec.url_env is not None
            resolved_url = os.environ.get(spec.url_env)
            if resolved_url is None:
                raise BlockweaverError("CONFIG_ENV_MISSING", f"RPC provider environment variable is not set: {spec.url_env}")
        _validate_url(resolved_url)
        return Provider(
            name,
            resolved_url,
            _positive_int(batch_size if batch_size is not None else spec.batch_size, "batch_size"),
            _positive_int(concurrency if concurrency is not None else spec.concurrency, "concurrency"),
            _positive_number(timeout if timeout is not None else spec.timeout, "timeout"),
        )


def default_config_path() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "blockweaver" / "config.toml"
    if sys.platform == "win32" and (appdata := os.environ.get("APPDATA")):
        return Path(appdata) / "blockweaver" / "config.toml"
    return Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "blockweaver" / "config.toml"


def selected_config_path(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit.expanduser().resolve()
    if configured := os.environ.get("BLOCKWEAVER_CONFIG"):
        return Path(configured).expanduser().resolve()
    return default_config_path().resolve()


def load_config(path: Path) -> Config:
    try:
        with path.open("rb") as stream:
            document = tomllib.load(stream)
    except FileNotFoundError:
        raise BlockweaverError("CONFIG_NOT_FOUND", f"Configuration file does not exist: {path}") from None
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise BlockweaverError("CONFIG_INVALID", f"Cannot read configuration: {error}") from None
    try:
        _keys(document, {"defaults", "chains", "providers"}, "configuration", required={"defaults", "chains", "providers"})
        defaults_raw = _table(document["defaults"], "defaults")
        _keys(
            defaults_raw,
            {"chain", "source", "provider", "verifier", "output_root", "format", "features"},
            "defaults",
            required={"chain", "source", "provider", "verifier", "output_root", "format", "features"},
        )
        source = _string(defaults_raw["source"], "defaults.source")
        if source != "rpc":
            raise BlockweaverError("SOURCE_UNAVAILABLE", f"Source is not available in this installation: {source}")
        output_format = _string(defaults_raw["format"], "defaults.format")
        if output_format not in {"parquet", "csv"}:
            raise ValueError("defaults.format must be parquet or csv")
        feature_values = defaults_raw["features"]
        if not isinstance(feature_values, list) or any(not isinstance(item, str) for item in feature_values):
            raise ValueError("defaults.features must be an array of strings")
        plan_features(feature_values)
        defaults = Defaults(
            _name(defaults_raw["chain"], "defaults.chain"),
            "rpc",
            _name(defaults_raw["provider"], "defaults.provider"),
            _name(defaults_raw["verifier"], "defaults.verifier"),
            Path(_string(defaults_raw["output_root"], "defaults.output_root")).expanduser(),
            output_format,  # type: ignore[arg-type]
            tuple(feature_values),
        )
        chains = _parse_chains(document["chains"])
        providers = _parse_providers(document["providers"])
        if defaults.chain not in chains:
            raise ValueError("defaults.chain does not name a configured chain")
        for chain in chains.values():
            for provider_name in (chain.provider or defaults.provider, chain.verifier or defaults.verifier):
                if provider_name not in providers:
                    raise ValueError(f"chain {chain.name} names an unknown provider: {provider_name}")
        if defaults.provider not in providers or defaults.verifier not in providers:
            raise ValueError("defaults provider profiles must be configured")
        return Config(path, defaults, chains, providers)
    except BlockweaverError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise BlockweaverError("CONFIG_INVALID", str(error)) from None


def _parse_chains(value: object) -> dict[str, Chain]:
    table = _table(value, "chains")
    if not table:
        raise ValueError("chains must not be empty")
    chains: dict[str, Chain] = {}
    for raw_name, raw_value in table.items():
        name = _name(raw_name, "chain name")
        item = _table(raw_value, f"chains.{name}")
        _keys(item, {"chain_id", "finality_tag", "provider", "verifier"}, f"chains.{name}", required={"chain_id", "finality_tag"})
        finality_tag = _string(item["finality_tag"], f"chains.{name}.finality_tag")
        if finality_tag not in {"finalized", "safe"}:
            raise ValueError(f"chains.{name}.finality_tag must be finalized or safe")
        chains[name] = Chain(
            name,
            _positive_int(item["chain_id"], f"chains.{name}.chain_id"),
            finality_tag,  # type: ignore[arg-type]
            _name(item["provider"], f"chains.{name}.provider") if "provider" in item else None,
            _name(item["verifier"], f"chains.{name}.verifier") if "verifier" in item else None,
        )
    return chains


def _parse_providers(value: object) -> dict[str, ProviderSpec]:
    table = _table(value, "providers")
    if not table:
        raise ValueError("providers must not be empty")
    providers: dict[str, ProviderSpec] = {}
    for raw_name, raw_value in table.items():
        name = _name(raw_name, "provider name")
        item = _table(raw_value, f"providers.{name}")
        _keys(item, {"url", "url_env", "batch_size", "concurrency", "timeout"}, f"providers.{name}")
        if ("url" in item) == ("url_env" in item):
            raise ValueError(f"providers.{name} must define exactly one of url or url_env")
        url = _string(item["url"], f"providers.{name}.url") if "url" in item else None
        if url is not None:
            _validate_url(url)
        url_env = _string(item["url_env"], f"providers.{name}.url_env") if "url_env" in item else None
        if url_env is not None and _ENV.fullmatch(url_env) is None:
            raise ValueError(f"providers.{name}.url_env is not a valid environment name")
        providers[name] = ProviderSpec(
            name,
            url,
            url_env,
            _positive_int(item.get("batch_size", 20), f"providers.{name}.batch_size"),
            _positive_int(item.get("concurrency", 6), f"providers.{name}.concurrency"),
            _positive_number(item.get("timeout", 30), f"providers.{name}.timeout"),
        )
    return providers


@dataclass(frozen=True, slots=True)
class RequestedRange:
    kind: Literal["block", "time"]
    start: int
    end: int
    from_value: int | str
    to_value: int | str

    def document(self) -> dict[str, object]:
        value: dict[str, object] = {"kind": self.kind, "from": self.from_value, "to": self.to_value}
        if self.kind == "time":
            value["normalized_from_utc"] = format_utc(self.start)
            value["normalized_to_utc"] = format_utc(self.end)
        return value


@dataclass(frozen=True, slots=True)
class ResolvedRange:
    first_block: int
    last_block: int
    first_timestamp: int
    last_timestamp: int

    def document(self) -> dict[str, int]:
        return {
            "from_block": self.first_block,
            "to_block": self.last_block,
            "from_timestamp": self.first_timestamp,
            "to_timestamp": self.last_timestamp,
        }


def requested_range(
    from_block: int | None,
    to_block: int | None,
    from_time: str | None,
    to_time: str | None,
) -> RequestedRange:
    block_used = from_block is not None or to_block is not None
    time_used = from_time is not None or to_time is not None
    if block_used == time_used:
        raise BlockweaverError("RANGE_INVALID", "Specify exactly one complete block or time range")
    if block_used:
        if from_block is None or to_block is None:
            raise BlockweaverError("RANGE_INVALID", "Both --from-block and --to-block are required")
        if from_block < 0 or to_block < 0 or from_block > _INT64_MAX or to_block > _INT64_MAX:
            raise BlockweaverError("RANGE_INVALID", "Block bounds must be signed Int64 nonnegative integers")
        if from_block > to_block:
            raise BlockweaverError("RANGE_INVALID", "Range start must not exceed range end")
        return RequestedRange("block", from_block, to_block, from_block, to_block)
    assert from_time is not None and to_time is not None
    start = parse_time(from_time, end=False)
    end = parse_time(to_time, end=True)
    if start > end:
        raise BlockweaverError("RANGE_INVALID", "Range start must not exceed range end")
    return RequestedRange("time", start, end, from_time, to_time)


def parse_time(value: str, *, end: bool) -> int:
    try:
        if _DATE.fullmatch(value):
            parsed = datetime.combine(date.fromisoformat(value), datetime.min.time(), UTC)
            precision = "date"
        else:
            match = _DATETIME.fullmatch(value)
            if match is None:
                raise ValueError
            date_part, hour, minute, second, zone = match.groups()
            minute_value = int(minute or 0)
            second_value = int(second or 0)
            parsed = datetime.fromisoformat(f"{date_part}T{hour}:{minute_value:02d}:{second_value:02d}{'+00:00' if zone == 'Z' else zone}")
            precision = "second" if second is not None else "minute" if minute is not None else "hour"
        if end:
            parsed += {"date": timedelta(days=1), "hour": timedelta(hours=1), "minute": timedelta(minutes=1), "second": timedelta(seconds=1)}[precision]
            parsed -= timedelta(seconds=1)
        timestamp = int(parsed.timestamp())
        if timestamp < 0 or timestamp > _INT64_MAX:
            raise ValueError
        return timestamp
    except (OverflowError, ValueError):
        raise BlockweaverError(
            "RANGE_INVALID",
            f"Invalid ISO 8601 time bound: {value}; use YYYY-MM-DD or a timezone-aware hour, minute, or second",
        ) from None


def format_utc(timestamp: int, *, filename: bool = False) -> str:
    value = datetime.fromtimestamp(timestamp, UTC)
    return value.strftime("%Y%m%dT%H%M%SZ" if filename else "%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class Header:
    block_number: int
    block_hash: str
    parent_hash: str
    timestamp: int
    values: dict[str, Value]

    def row(self, plan: Plan, fees: dict[int, int] | None = None) -> dict[str, Value]:
        result: dict[str, Value] = {"block_number": self.block_number}
        fees = fees or {}
        for feature in plan.features:
            if feature.percentile is not None:
                result[feature.name] = fees[feature.percentile]
            elif feature.name == "block_hash":
                result[feature.name] = self.block_hash
            elif feature.name == "parent_hash":
                result[feature.name] = self.parent_hash
            elif feature.name == "timestamp":
                result[feature.name] = self.timestamp
            else:
                result[feature.name] = self.values[feature.name]
        return result


@dataclass(frozen=True, slots=True)
class Anchor:
    block_number: int
    block_hash: str
    tag: str

    def document(self) -> dict[str, object]:
        return {"block_number": self.block_number, "block_hash": self.block_hash, "tag": self.tag}


def quantity(value: Any, label: str) -> int:
    if not isinstance(value, str) or _QUANTITY.fullmatch(value) is None:
        raise ValueError(f"Invalid {label} quantity")
    parsed = int(value, 16)
    if parsed > _INT64_MAX:
        raise ValueError(f"{label} quantity exceeds signed Int64")
    return parsed


def block_hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise ValueError(f"Invalid {label}")
    return value


def parse_header(value: Any, *, expected: int | None, plan: Plan) -> Header:
    if not isinstance(value, dict):
        raise ValueError("Invalid block response shape")
    if any(field not in value for field in plan.header_fields):
        raise ValueError("Block response is missing required fields")
    number = quantity(value["number"], "block number")
    if expected is not None and number != expected:
        raise ValueError(f"RPC returned block {number} when {expected} was requested")
    timestamp = quantity(value["timestamp"], "timestamp")
    values: dict[str, Value] = {}
    selected = {feature.name for feature in plan.features}
    if "baseFeePerGas" in plan.header_fields:
        base_fee = quantity(value["baseFeePerGas"], "base fee")
        if base_fee <= 0:
            raise ValueError("Invalid base fee domain")
        if "base_fee_per_gas" in selected:
            values["base_fee_per_gas"] = base_fee
    if "gasUsed" in plan.header_fields:
        gas_used = quantity(value["gasUsed"], "gas used")
        gas_limit = quantity(value["gasLimit"], "gas limit")
        if gas_limit <= 0 or gas_used > gas_limit:
            raise ValueError("Invalid gas domain")
        if "gas_used" in selected:
            values["gas_used"] = gas_used
        if "gas_limit" in selected:
            values["gas_limit"] = gas_limit
    if "transactions" in plan.header_fields:
        transactions = value["transactions"]
        if not isinstance(transactions, list) or any(not isinstance(item, str) or _HASH.fullmatch(item) is None for item in transactions):
            raise ValueError("Invalid transactions field")
        values["tx_count"] = len(transactions)
    return Header(number, block_hash(value["hash"], "block hash"), block_hash(value["parentHash"], "parent hash"), timestamp, values)


def validate_links(headers: list[Header], previous: Header | None = None) -> None:
    for header in headers:
        if previous is not None:
            if header.block_number != previous.block_number + 1:
                raise ValueError("Blocks are not contiguous")
            if header.parent_hash != previous.block_hash:
                raise ValueError(f"Parent link mismatch at block {header.block_number}")
            if header.timestamp < previous.timestamp:
                raise ValueError(f"Timestamp decreases at block {header.block_number}")
        previous = header


def validate_uuid(value: UUID) -> None:
    if value.version != 4 or str(value) != str(value).lower():
        raise BlockweaverError("REQUEST_INVALID", "Dataset ID must be a canonical UUID4")


def _table(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a table")
    return value


def _keys(
    value: dict[str, Any],
    allowed: set[str],
    label: str,
    *,
    required: set[str] | frozenset[str] = frozenset(),
) -> None:
    unknown = sorted(set(value) - allowed)
    missing = sorted(required - set(value))
    if unknown:
        raise ValueError(f"Unknown {label} key: {unknown[0]}")
    if missing:
        raise ValueError(f"Missing {label} key: {missing[0]}")


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a nonempty string")
    return value


def _name(value: object, label: str) -> str:
    parsed = _string(value, label)
    if _NAME.fullmatch(parsed) is None:
        raise ValueError(f"{label} must contain lowercase letters, digits, underscores, or hyphens")
    return parsed


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0 or value > _INT64_MAX:
        raise ValueError(f"{label} must be a positive signed Int64 integer")
    return value


def _positive_number(value: object, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{label} must be positive")
    return float(value)


def _validate_url(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise BlockweaverError("CONFIG_INVALID", "RPC provider URL must be an absolute HTTP or HTTPS URL")
