"""Configuration, feature, range, and RPC value contracts."""

from __future__ import annotations

import os
import re
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from ipaddress import ip_address
from math import isfinite
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

OutputFormat = Literal["parquet", "csv"]
Source = Literal["rpc", "bigquery"]
Value = int | str

_INT64_MAX = 2**63 - 1
_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*\Z")
_ENV = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_DATETIME = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2})(?::(\d{2})(?::(\d{2}))?)?(Z|[+-]\d{2}:\d{2})\Z")
_PROJECT = re.compile(r"[a-z][a-z0-9-]{4,61}[a-z0-9]\Z")
_DATASET = re.compile(r"([a-z][a-z0-9-]{4,61}[a-z0-9])\.([A-Za-z_][A-Za-z0-9_]{0,1023})\Z")


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
    bigquery_family: Literal["blocks", "transactions", "receipts"] = "blocks"
    bigquery_field: str | None = None
    bigquery_dependencies: tuple[str, ...] = ()
    percentile: int | None = None
    domain: str = ""

    def document(self, *, available_sources: tuple[Source, ...] = ("rpc",)) -> dict[str, object]:
        return {
            "name": self.name,
            "type": self.dtype,
            "unit": self.unit,
            "source_support": ["rpc", "bigquery"],
            "acquisition_families": {"rpc": self.family, "bigquery": self.bigquery_family},
            "domain_rule": self.domain,
            "hidden_dependencies": {"rpc": list(self.dependencies), "bigquery": list(self.bigquery_dependencies)},
            "configured_sources": list(available_sources),
        }


FEATURES = (
    Feature("timestamp", "Int64", "unix_second", "header", "timestamp", bigquery_field="block_timestamp", domain="nonnegative, nondecreasing"),
    Feature("block_hash", "UTF-8", "hex", "header", "hash", bigquery_field="block_hash", domain="lowercase 0x-prefixed 32-byte hash"),
    Feature("parent_hash", "UTF-8", "hex", "header", "parentHash", bigquery_field="parent_hash", domain="lowercase 0x-prefixed 32-byte hash"),
    Feature("base_fee_per_gas", "Int64", "wei/gas", "header", "baseFeePerGas", bigquery_field="base_fee_per_gas", domain="positive"),
    Feature(
        "gas_used",
        "Int64",
        "gas",
        "header",
        "gasUsed",
        ("gasLimit",),
        bigquery_field="gas_used",
        bigquery_dependencies=("gas_limit",),
        domain="nonnegative and at most gas_limit",
    ),
    Feature(
        "gas_limit",
        "Int64",
        "gas",
        "header",
        "gasLimit",
        ("gasUsed",),
        bigquery_field="gas_limit",
        bigquery_dependencies=("gas_used",),
        domain="positive",
    ),
    Feature("tx_count", "Int64", "transaction", "header", "transactions", bigquery_family="transactions", domain="nonnegative"),
    Feature(
        "effective_priority_fee_per_gas_p50",
        "Int64",
        "wei/gas",
        "fee_history",
        bigquery_family="receipts",
        percentile=50,
        domain="nonnegative",
    ),
    Feature(
        "effective_priority_fee_per_gas_p90",
        "Int64",
        "wei/gas",
        "fee_history",
        bigquery_family="receipts",
        percentile=90,
        domain="nonnegative",
    ),
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

    def document(self, source: Source = "rpc") -> dict[str, object]:
        return _rpc_plan_document(self) if source == "rpc" else _bigquery_plan_document(self)


def _rpc_plan_document(plan: Plan) -> dict[str, object]:
    families: list[dict[str, object]] = [{"family": "header", "method": "eth_getBlockByNumber", "fields": list(plan.header_fields)}]
    if plan.percentiles:
        families.append({"family": "fee_history", "method": "eth_feeHistory", "reward_percentiles": list(plan.percentiles)})
    return {"families": families}


def _bigquery_plan_document(plan: Plan) -> dict[str, object]:
    fields = {"block_number", "block_timestamp", "block_hash", "parent_hash"}
    fields.update(feature.bigquery_field for feature in plan.features if feature.bigquery_field is not None)
    fields.update(dependency for feature in plan.features for dependency in feature.bigquery_dependencies)
    if plan.percentiles:
        fields.update({"base_fee_per_gas", "gas_used"})
    families: list[dict[str, object]] = [{"family": "blocks", "table": "blocks", "fields": sorted(fields)}]
    if any(feature.bigquery_family == "transactions" for feature in plan.features):
        families.append({"family": "transactions", "table": "transactions", "fields": ["block_number", "block_timestamp"]})
    if any(feature.bigquery_family == "receipts" for feature in plan.features):
        families.append(
            {
                "family": "receipts",
                "table": "receipts",
                "fields": ["block_number", "block_hash", "block_timestamp", "effective_gas_price", "gas_used", "transaction_index"],
                "reward_percentiles": list(plan.percentiles),
            }
        )
    return {"families": families}


def parse_source(value: object) -> Source:
    if value == "rpc":
        return "rpc"
    if value == "bigquery":
        return "bigquery"
    raise BlockweaverError("SOURCE_UNAVAILABLE", f"Unknown source: {value}")


def configured_sources(config: Config, chain: Chain) -> tuple[Source, ...]:
    verifier = chain.verifier or config.defaults.verifier
    sources: list[Source] = []
    primary = chain.provider or config.defaults.provider
    if primary is not None and primary in config.providers and verifier in config.providers:
        sources.append("rpc")
    if chain.bigquery_dataset is not None and config.bigquery is not None and verifier in config.providers:
        sources.append("bigquery")
    return tuple(sources)


def validate_manifest_source(value: object) -> Source:
    if not isinstance(value, dict):
        raise ValueError("Invalid manifest source")
    source = value.get("type")
    if source == "rpc":
        if set(value) != {"type", "provider", "verifier"} or any(
            not isinstance(value[field], str) or _NAME.fullmatch(value[field]) is None for field in ("provider", "verifier")
        ):
            raise ValueError("Invalid manifest source")
        return "rpc"
    if source == "bigquery":
        if (
            set(value) != {"type", "dataset", "verifier"}
            or not isinstance(value["dataset"], str)
            or not isinstance(value["verifier"], str)
            or _NAME.fullmatch(value["verifier"]) is None
        ):
            raise ValueError("Invalid manifest source")
        validate_dataset_identifier(value["dataset"])
        return "bigquery"
    raise ValueError("Invalid manifest source")


def validate_verification(value: object, chain_id: int, source: Source) -> dict[str, object]:
    chain_fields = {"primary_chain_id", "verifier_chain_id"} if source == "rpc" else {"verifier_chain_id"}
    nonnegative_fields = set() if source == "rpc" else {"dry_run_bytes"}
    fields = {"target_agreement", "sampled_blocks", *chain_fields, *nonnegative_fields}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("Invalid verification facts")
    if value["target_agreement"] is not True or any(value[field] != chain_id for field in chain_fields):
        raise ValueError("Invalid verification facts")
    if any(type(value[field]) is not int or value[field] < 0 for field in nonnegative_fields):
        raise ValueError("Invalid verification facts")
    return value


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
        try:
            _name(self.name, "provider name")
            _validate_url(self.url)
            _positive_int(self.batch_size, "batch_size")
            _positive_int(self.concurrency, "concurrency")
            _positive_number(self.timeout, "timeout")
        except ValueError as error:
            raise BlockweaverError("CONFIG_INVALID", str(error)) from None


@dataclass(frozen=True, slots=True)
class Chain:
    name: str
    chain_id: int
    finality_tag: Literal["finalized", "safe"]
    provider: str | None
    verifier: str | None
    bigquery_dataset: str | None


@dataclass(frozen=True, slots=True)
class Defaults:
    chain: str
    source: Source
    provider: str | None
    verifier: str
    output_root: Path
    output_format: OutputFormat
    features: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BigQuerySpec:
    project: str | None
    project_env: str | None
    maximum_bytes_billed: int

    def resolve(self) -> BigQuerySettings:
        project = self.project
        if project is None:
            assert self.project_env is not None
            project = os.environ.get(self.project_env)
            if project is None:
                raise BlockweaverError("CONFIG_ENV_MISSING", f"BigQuery project environment variable is not set: {self.project_env}")
        try:
            validate_project(project)
        except ValueError as error:
            raise BlockweaverError("CONFIG_INVALID", str(error)) from None
        return BigQuerySettings(project, self.maximum_bytes_billed)


@dataclass(frozen=True, slots=True)
class BigQuerySettings:
    project: str
    maximum_bytes_billed: int


@dataclass(frozen=True, slots=True)
class Config:
    path: Path
    defaults: Defaults
    chains: dict[str, Chain]
    providers: dict[str, ProviderSpec]
    bigquery: BigQuerySpec | None

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

    def bigquery_settings(self) -> BigQuerySettings:
        if self.bigquery is None:
            raise BlockweaverError("SOURCE_UNAVAILABLE", "BigQuery source requires a [bigquery] configuration")
        return self.bigquery.resolve()


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
        _keys(document, {"defaults", "chains", "providers", "bigquery"}, "configuration", required={"defaults", "chains", "providers"})
        defaults_raw = _table(document["defaults"], "defaults")
        _keys(
            defaults_raw,
            {"chain", "source", "provider", "verifier", "output_root", "format", "features"},
            "defaults",
            required={"chain", "source", "verifier", "output_root", "format", "features"},
        )
        source = parse_source(_string(defaults_raw["source"], "defaults.source"))
        output_format = _string(defaults_raw["format"], "defaults.format")
        if output_format not in {"parquet", "csv"}:
            raise ValueError("defaults.format must be parquet or csv")
        feature_values = defaults_raw["features"]
        if not isinstance(feature_values, list) or any(not isinstance(item, str) for item in feature_values):
            raise ValueError("defaults.features must be an array of strings")
        plan_features(feature_values)
        defaults = Defaults(
            _name(defaults_raw["chain"], "defaults.chain"),
            source,  # type: ignore[arg-type]
            _name(defaults_raw["provider"], "defaults.provider") if "provider" in defaults_raw else None,
            _name(defaults_raw["verifier"], "defaults.verifier"),
            Path(_string(defaults_raw["output_root"], "defaults.output_root")).expanduser(),
            output_format,  # type: ignore[arg-type]
            tuple(feature_values),
        )
        chains = _parse_chains(document["chains"])
        providers = _parse_providers(document["providers"])
        bigquery = _parse_bigquery(document["bigquery"]) if "bigquery" in document else None
        if defaults.chain not in chains:
            raise ValueError("defaults.chain does not name a configured chain")
        for chain in chains.values():
            for provider_name in (chain.provider, chain.verifier):
                if provider_name is not None and provider_name not in providers:
                    raise ValueError(f"chain {chain.name} names an unknown provider: {provider_name}")
        if defaults.provider is not None and defaults.provider not in providers:
            raise ValueError("defaults.provider must name a configured provider")
        if defaults.verifier not in providers:
            raise ValueError("defaults.verifier must name a configured provider")
        config = Config(path, defaults, chains, providers, bigquery)
        if defaults.source not in configured_sources(config, chains[defaults.chain]):
            raise ValueError(f"default {defaults.source} source is not fully configured")
        return config
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
        _keys(
            item,
            {"chain_id", "finality_tag", "provider", "verifier", "bigquery_dataset"},
            f"chains.{name}",
            required={"chain_id", "finality_tag"},
        )
        finality_tag = _string(item["finality_tag"], f"chains.{name}.finality_tag")
        if finality_tag not in {"finalized", "safe"}:
            raise ValueError(f"chains.{name}.finality_tag must be finalized or safe")
        chains[name] = Chain(
            name,
            _positive_int(item["chain_id"], f"chains.{name}.chain_id"),
            finality_tag,  # type: ignore[arg-type]
            _name(item["provider"], f"chains.{name}.provider") if "provider" in item else None,
            _name(item["verifier"], f"chains.{name}.verifier") if "verifier" in item else None,
            validate_dataset_identifier(_string(item["bigquery_dataset"], f"chains.{name}.bigquery_dataset")) if "bigquery_dataset" in item else None,
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


def _parse_bigquery(value: object) -> BigQuerySpec:
    item = _table(value, "bigquery")
    _keys(item, {"project", "project_env", "maximum_bytes_billed"}, "bigquery", required={"maximum_bytes_billed"})
    if ("project" in item) == ("project_env" in item):
        raise ValueError("bigquery must define exactly one of project or project_env")
    project = _string(item["project"], "bigquery.project") if "project" in item else None
    if project is not None:
        validate_project(project)
    project_env = _string(item["project_env"], "bigquery.project_env") if "project_env" in item else None
    if project_env is not None and _ENV.fullmatch(project_env) is None:
        raise ValueError("bigquery.project_env is not a valid environment name")
    return BigQuerySpec(project, project_env, _positive_int(item["maximum_bytes_billed"], "bigquery.maximum_bytes_billed"))


def validate_dataset_identifier(value: str) -> str:
    if _DATASET.fullmatch(value) is None:
        raise ValueError("BigQuery dataset must be a project.dataset identifier")
    return value


def validate_project(value: str) -> str:
    if _PROJECT.fullmatch(value) is None:
        raise ValueError("BigQuery billing project is not a valid project ID")
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


@dataclass(frozen=True, slots=True, kw_only=True)
class RpcDownloadRequest:
    dataset_id: UUID
    chain: Chain
    requested_range: RequestedRange
    plan: Plan
    output_root: Path
    output_format: OutputFormat
    primary: Provider
    verifier: Provider

    def __post_init__(self) -> None:
        validate_uuid(self.dataset_id)
        if self.primary.name == self.verifier.name:
            raise BlockweaverError("PROVIDER_INVALID", "Primary and verifier must be distinct provider profiles")
        if self.primary.url == self.verifier.url:
            raise BlockweaverError("PROVIDER_INVALID", "Primary and verifier RPC endpoints must be independent")

    @property
    def source(self) -> Literal["rpc"]:
        return "rpc"

    def source_document(self) -> dict[str, object]:
        return {"type": "rpc", "provider": self.primary.name, "verifier": self.verifier.name}

    def secrets(self) -> list[str]:
        return [self.primary.url, self.verifier.url]


@dataclass(frozen=True, slots=True, kw_only=True)
class BigQueryDownloadRequest:
    dataset_id: UUID
    chain: Chain
    requested_range: RequestedRange
    plan: Plan
    output_root: Path
    output_format: OutputFormat
    dataset: str
    bigquery: BigQuerySettings
    verifier: Provider

    def __post_init__(self) -> None:
        validate_uuid(self.dataset_id)

    @property
    def source(self) -> Literal["bigquery"]:
        return "bigquery"

    def source_document(self) -> dict[str, object]:
        return {"type": "bigquery", "dataset": self.dataset, "verifier": self.verifier.name}

    def secrets(self) -> list[str]:
        return [self.bigquery.project, self.verifier.url]


DownloadRequest = RpcDownloadRequest | BigQueryDownloadRequest


def resolve_download_request(
    config: Config,
    *,
    dataset_id: UUID,
    chain: str | None,
    source: Source | None,
    provider: str | None,
    verifier: str | None,
    rpc_url: str | None,
    verify_rpc_url: str | None,
    output_root: Path | None,
    output_format: OutputFormat | None,
    features: list[str] | None,
    from_block: int | None,
    to_block: int | None,
    from_time: str | None,
    to_time: str | None,
    batch_size: int | None,
    concurrency: int | None,
    timeout: float | None,
) -> DownloadRequest:
    selected_chain = config.chain(chain)
    selected_source = parse_source(source or config.defaults.source)
    if selected_source not in configured_sources(config, selected_chain):
        raise BlockweaverError("SOURCE_UNAVAILABLE", f"Chain {selected_chain.name} does not configure source={selected_source}")
    plan = plan_features(features if features is not None else config.defaults.features)
    bounds = requested_range(from_block, to_block, from_time, to_time)
    root = (output_root or config.defaults.output_root).expanduser()
    selected_format = output_format or config.defaults.output_format
    verifier_name = verifier or selected_chain.verifier or config.defaults.verifier
    if selected_source == "bigquery":
        if provider is not None or rpc_url is not None:
            raise BlockweaverError("SOURCE_OPTION_INVALID", "--provider and --rpc-url do not apply to source=bigquery")
        verifying = config.provider(
            verifier_name,
            url=verify_rpc_url,
            batch_size=batch_size,
            concurrency=concurrency,
            timeout=timeout,
        )
        if selected_chain.bigquery_dataset is None:
            raise BlockweaverError("SOURCE_UNAVAILABLE", "bigquery source is not fully configured")
        return BigQueryDownloadRequest(
            dataset_id=dataset_id,
            chain=selected_chain,
            requested_range=bounds,
            plan=plan,
            output_root=root,
            output_format=selected_format,
            dataset=selected_chain.bigquery_dataset,
            bigquery=config.bigquery_settings(),
            verifier=verifying,
        )
    primary_name = provider or selected_chain.provider or config.defaults.provider
    if primary_name is None:
        raise BlockweaverError("SOURCE_UNAVAILABLE", "rpc source requires a primary provider")
    return RpcDownloadRequest(
        dataset_id=dataset_id,
        chain=selected_chain,
        requested_range=bounds,
        plan=plan,
        output_root=root,
        output_format=selected_format,
        primary=config.provider(primary_name, url=rpc_url, batch_size=batch_size, concurrency=concurrency, timeout=timeout),
        verifier=config.provider(verifier_name, url=verify_rpc_url, batch_size=batch_size, concurrency=concurrency, timeout=timeout),
    )


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
        if not isinstance(transactions, list):
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
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not isfinite(value) or value <= 0 or value > 3600:
        raise BlockweaverError("CONFIG_INVALID", f"{label} must be finite and between 0 and 3600 seconds")
    return float(value)


def _validate_url(value: str) -> None:
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        raise BlockweaverError("CONFIG_INVALID", "RPC provider URL is malformed") from None
    if (
        any(character.isspace() for character in value)
        or parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or hostname is None
        or parsed.fragment
        or parsed.netloc.endswith(":")
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise BlockweaverError("CONFIG_INVALID", "RPC provider URL must be an absolute HTTP or HTTPS URL")
    try:
        ip_address(hostname)
    except ValueError:
        try:
            encoded = hostname.encode("idna").decode("ascii")
        except UnicodeError:
            raise BlockweaverError("CONFIG_INVALID", "RPC provider URL has an invalid hostname") from None
        labels = encoded.removesuffix(".").split(".")
        if len(encoded) > 253 or any(
            not label or len(label) > 63 or re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", label) is None for label in labels
        ):
            raise BlockweaverError("CONFIG_INVALID", "RPC provider URL has an invalid hostname") from None
