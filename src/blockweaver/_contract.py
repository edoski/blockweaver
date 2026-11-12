"""What a dataset is: errors, value types, the feature registry, plans, headers and ranges."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, ClassVar, Literal

OutputFormat = Literal["parquet", "csv"]


Source = Literal["rpc", "bigquery"]


Value = int | str


Row = dict[str, Value]
Progress = Callable[[dict[str, object]], None]


INT64_MAX = 2**63 - 1


HASH_PATTERN = "0x[0-9a-f]{64}"


HASH = re.compile(HASH_PATTERN + r"\Z")


NAME = re.compile(r"[a-z0-9][a-z0-9_-]*\Z")


_DATASET = re.compile(r"([a-z][a-z0-9-]{4,61}[a-z0-9])\.([A-Za-z_][A-Za-z0-9_]{0,1023})\Z")


class BlockweaverError(Exception):
    """Expected failure with a stable machine-readable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class Domain(Enum):
    """The values a column may hold. Each member's value is its published rule text."""

    NONNEGATIVE = "nonnegative"
    POSITIVE = "positive"
    HASH = "lowercase 0x-prefixed 32-byte hash"

    def admits(self, value: object) -> bool:
        if self is Domain.HASH:
            return isinstance(value, str) and HASH.fullmatch(value) is not None
        return type(value) is int and (self is Domain.NONNEGATIVE or value > 0) and 0 <= value <= INT64_MAX


@dataclass(frozen=True, slots=True)
class HeaderKey:
    """An eth_getBlockByNumber key and how its value becomes a column value."""

    key: str
    kind: Literal["quantity", "hash", "count"]
    family: ClassVar[str] = "header"


@dataclass(frozen=True, slots=True)
class FeeHistory:
    percentile: int
    family: ClassVar[str] = "fee_history"


@dataclass(frozen=True, slots=True)
class BlocksColumn:
    column: str
    type: Literal["INTEGER", "STRING", "TIMESTAMP"]
    family: ClassVar[str] = "blocks"


@dataclass(frozen=True, slots=True)
class TransactionRows:
    family: ClassVar[str] = "transactions"


@dataclass(frozen=True, slots=True)
class ReceiptPercentile:
    percentile: int
    family: ClassVar[str] = "receipts"


@dataclass(frozen=True, slots=True)
class Feature:
    """One selectable column: its type, its domain, and how each source acquires it."""

    name: str
    dtype: Literal["Int64", "UTF-8"]
    unit: str
    domain: Domain
    rpc: HeaderKey | FeeHistory
    bigquery: BlocksColumn | TransactionRows | ReceiptPercentile
    at_most: str | None = None
    nondecreasing: bool = False

    @property
    def rule(self) -> str:
        return self.domain.value + (", nondecreasing" if self.nondecreasing else "") + (f" and at most {self.at_most}" if self.at_most else "")

    def document(self) -> dict[str, object]:
        partners = _bound_partners(self)
        return {
            "name": self.name,
            "type": self.dtype,
            "unit": self.unit,
            "supported_sources": ["rpc", "bigquery"],
            "acquisition_families": {"rpc": self.rpc.family, "bigquery": self.bigquery.family},
            "domain_rule": self.rule,
            "hidden_dependencies": {
                "rpc": [partner.rpc.key for partner in partners if isinstance(partner.rpc, HeaderKey)],
                "bigquery": [partner.bigquery.column for partner in partners if isinstance(partner.bigquery, BlocksColumn)],
            },
        }


TIMESTAMP = Feature(
    "timestamp",
    "Int64",
    "unix_second",
    Domain.NONNEGATIVE,
    HeaderKey("timestamp", "quantity"),
    BlocksColumn("block_timestamp", "TIMESTAMP"),
    nondecreasing=True,
)


BLOCK_HASH = Feature("block_hash", "UTF-8", "hex", Domain.HASH, HeaderKey("hash", "hash"), BlocksColumn("block_hash", "STRING"))


PARENT_HASH = Feature("parent_hash", "UTF-8", "hex", Domain.HASH, HeaderKey("parentHash", "hash"), BlocksColumn("parent_hash", "STRING"))


CORE = (TIMESTAMP, BLOCK_HASH, PARENT_HASH)


FEATURES = (
    *CORE,
    Feature("base_fee_per_gas", "Int64", "wei/gas", Domain.POSITIVE, HeaderKey("baseFeePerGas", "quantity"), BlocksColumn("base_fee_per_gas", "INTEGER")),
    Feature("gas_used", "Int64", "gas", Domain.NONNEGATIVE, HeaderKey("gasUsed", "quantity"), BlocksColumn("gas_used", "INTEGER"), at_most="gas_limit"),
    Feature("gas_limit", "Int64", "gas", Domain.POSITIVE, HeaderKey("gasLimit", "quantity"), BlocksColumn("gas_limit", "INTEGER")),
    Feature("tx_count", "Int64", "transaction", Domain.NONNEGATIVE, HeaderKey("transactions", "count"), TransactionRows()),
    *(
        Feature(f"effective_priority_fee_per_gas_p{percentile}", "Int64", "wei/gas", Domain.NONNEGATIVE, FeeHistory(percentile), ReceiptPercentile(percentile))
        for percentile in (50, 90)
    ),
)


FEATURE_BY_NAME = {feature.name: feature for feature in FEATURES}


def _bound_partners(feature: Feature) -> list[Feature]:
    """Features that share an at_most bound with this one. Both sides are acquired so the bound can be checked."""
    return [other for other in FEATURES if other.name == feature.at_most or other.at_most == feature.name]


@dataclass(frozen=True, slots=True)
class Plan:
    """What one request exports, stores in checkpoints, and acquires from its source."""

    features: tuple[Feature, ...]
    stored: tuple[Feature, ...]
    acquired: tuple[Feature, ...]

    @property
    def columns(self) -> tuple[str, ...]:
        return ("block_number", *(feature.name for feature in self.features))

    @property
    def schema(self) -> dict[str, str]:
        return {"block_number": "Int64", **{feature.name: feature.dtype for feature in self.features}}

    @property
    def stored_schema(self) -> dict[str, str]:
        return {"block_number": "Int64", **{feature.name: feature.dtype for feature in self.stored}}

    @property
    def percentiles(self) -> tuple[int, ...]:
        return tuple(feature.rpc.percentile for feature in self.acquired if isinstance(feature.rpc, FeeHistory))

    def schema_document(self) -> list[dict[str, str]]:
        return [
            {"name": "block_number", "type": "Int64", "unit": "block"},
            *({"name": feature.name, "type": feature.dtype, "unit": feature.unit} for feature in self.features),
        ]

    def document(self, source: Source) -> dict[str, object]:
        if source == "bigquery":
            families: list[dict[str, object]] = [{"family": table, "table": table, "fields": sorted(fields)} for table, fields in bigquery_tables(self).items()]
            if self.percentiles:
                families[-1]["reward_percentiles"] = list(self.percentiles)
            return {"families": families}
        keys = [feature.rpc.key for feature in (BLOCK_HASH, PARENT_HASH, TIMESTAMP, *self.acquired[len(CORE) :]) if isinstance(feature.rpc, HeaderKey)]
        families = [{"family": HeaderKey.family, "method": "eth_getBlockByNumber", "fields": ["number", *keys]}]
        if self.percentiles:
            families.append({"family": FeeHistory.family, "method": "eth_feeHistory", "reward_percentiles": list(self.percentiles)})
        return {"families": families}

    def row(self, values: Mapping[str, Any]) -> Row:
        """Check one block's acquired values against the registry and keep what checkpoints store."""
        for feature in self.acquired:
            if not feature.domain.admits(values[feature.name]):
                raise ValueError(f"Block {values['block_number']} has {feature.name} outside its domain: {feature.rule}")
        for feature in self.acquired:
            if feature.at_most is not None and values[feature.name] > values[feature.at_most]:
                raise ValueError(f"Block {values['block_number']} has {feature.name} outside its domain: {feature.rule}")
        return {name: values[name] for name in self.stored_schema}

    def export(self, row: Mapping[str, Value]) -> Row:
        return {name: row[name] for name in self.columns}


def bigquery_tables(plan: Plan) -> dict[str, dict[str, str]]:
    """The Google Blockchain Analytics tables and typed fields a plan reads."""
    blocks = {
        "block_number": "INTEGER",
        **{feature.bigquery.column: feature.bigquery.type for feature in plan.acquired if isinstance(feature.bigquery, BlocksColumn)},
    }
    tables = {"blocks": blocks}
    if any(isinstance(feature.bigquery, TransactionRows) for feature in plan.acquired):
        tables["transactions"] = {"block_hash": "STRING", "block_timestamp": "TIMESTAMP"}
    if plan.percentiles:
        blocks.update({"base_fee_per_gas": "INTEGER", "gas_used": "INTEGER"})
        tables["receipts"] = {
            "block_hash": "STRING",
            "block_timestamp": "TIMESTAMP",
            "effective_gas_price": "INTEGER",
            "gas_used": "INTEGER",
            "transaction_index": "INTEGER",
        }
    return tables


def plan_features(names: list[str] | tuple[str, ...]) -> Plan:
    if len(names) != len(set(names)):
        raise ValueError("Features must not contain duplicates")
    if "block_number" in names:
        raise ValueError("block_number is always included and must not be selected")
    unknown = sorted(set(names) - FEATURE_BY_NAME.keys())
    if unknown:
        raise ValueError(f"Unknown feature: {unknown[0]}")
    selected = tuple(feature for feature in FEATURES if feature.name in names)
    stored = tuple(feature for feature in FEATURES if feature in CORE or feature in selected)
    partners = [partner for feature in selected for partner in _bound_partners(feature)]
    return Plan(selected, stored, tuple(feature for feature in FEATURES if feature in stored or feature in partners))


def validate_dataset_identifier(value: str) -> str:
    if _DATASET.fullmatch(value) is None:
        raise ValueError("BigQuery dataset must be a project.dataset identifier")
    return value


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


def format_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class Header:
    """The integrity core of one block."""

    block_number: int
    block_hash: str
    parent_hash: str
    timestamp: int

    @classmethod
    def of(cls, row: Mapping[str, Any]) -> Header:
        return cls(row["block_number"], row[BLOCK_HASH.name], row[PARENT_HASH.name], row[TIMESTAMP.name])


@dataclass(frozen=True, slots=True)
class Anchor:
    block_number: int
    block_hash: str
    tag: str

    def document(self) -> dict[str, object]:
        return {"block_number": self.block_number, "block_hash": self.block_hash, "tag": self.tag}


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
