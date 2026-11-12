"""Google Blockchain Analytics source: the fixed query, its cost guard and its row parser."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from ._contract import (
    BlocksColumn,
    BlockweaverError,
    Plan,
    ReceiptPercentile,
    Row,
    TransactionRows,
    bigquery_tables,
)

BigQuerySchema = dict[str, tuple[str, str]]


@dataclass(frozen=True, slots=True)
class BigQueryPlan:
    sql: str
    table_fields: dict[str, dict[str, str]]
    result_schema: dict[str, str]
    checks: tuple[tuple[str, str, str], ...]


class BigQueryClient:
    def __init__(self, module: Any, project: str) -> None:
        self._module = module
        self._client = module.Client(project=project)

    def table_schema(self, dataset: str, table: str) -> BigQuerySchema:
        value = self._client.get_table(f"{dataset}.{table}")
        return {field.name: (field.field_type, field.mode) for field in value.schema}

    def dry_run(self, sql: str, parameters: dict[str, int]) -> tuple[int, BigQuerySchema]:
        config = self._config(parameters, dry_run=True)
        job = self._client.query(sql, job_config=config)
        if type(job.total_bytes_processed) is not int or job.total_bytes_processed < 0:
            raise BlockweaverError("BIGQUERY_INVALID", "BigQuery dry run did not return a byte estimate")
        return job.total_bytes_processed, {field.name: (field.field_type, field.mode) for field in job.schema}

    def pages(
        self,
        sql: str,
        parameters: dict[str, int],
        maximum_bytes_billed: int,
        page_size: int,
    ) -> Iterator[Iterator[dict[str, object]]]:
        config = self._config(parameters, maximum_bytes_billed=maximum_bytes_billed)
        rows = self._client.query(sql, job_config=config).result(page_size=page_size)
        for page in rows.pages:
            yield (dict(row.items()) for row in page)

    def _config(self, parameters: dict[str, int], *, dry_run: bool = False, maximum_bytes_billed: int | None = None) -> Any:
        options = {
            "dry_run": dry_run,
            "use_query_cache": False,
            "query_parameters": [self._module.ScalarQueryParameter(name, "INT64", value) for name, value in parameters.items()],
        }
        if maximum_bytes_billed is not None:
            options["maximum_bytes_billed"] = maximum_bytes_billed
        return self._module.QueryJobConfig(**options)


def open_bigquery(project: str) -> BigQueryClient:
    try:
        module = import_module("google.cloud.bigquery")
    except ModuleNotFoundError:
        raise BlockweaverError(
            "SOURCE_DEPENDENCY_MISSING",
            "BigQuery source requires the optional blockweaver[bigquery] dependency",
        ) from None
    try:
        return BigQueryClient(module, project)
    except Exception as error:
        raise BlockweaverError("BIGQUERY_FAILED", f"Cannot initialize BigQuery client: {type(error).__name__}") from None


def compile_bigquery(dataset: str, plan: Plan) -> BigQueryPlan:
    """The one fixed query shape. Receipts are a second witness: their gas must add up to the block's, and their count must equal the transaction count."""
    tables = bigquery_tables(plan)
    window = "block_timestamp BETWEEN TIMESTAMP_SECONDS(@from_timestamp) AND TIMESTAMP_SECONDS(@to_timestamp)"
    percentiles = [(feature.name, feature.bigquery.percentile) for feature in plan.acquired if isinstance(feature.bigquery, ReceiptPercentile)]
    counts = [feature for feature in plan.acquired if isinstance(feature.bigquery, TransactionRows)]
    ctes = [
        "requested_blocks AS (\n"
        f"  SELECT {', '.join(sorted(tables['blocks']))}\n"
        f"  FROM `{dataset}.blocks`\n"
        f"  WHERE block_number BETWEEN @first_block AND @last_block\n    AND {window}\n"
        ")"
    ]
    joins: list[str] = []
    projections = ["b.block_number AS block_number"]
    checks: list[tuple[str, str, str]] = []
    if counts:
        ctes.append(f"tx_counts AS (\n  SELECT block_hash, COUNT(*) AS tx_count\n  FROM `{dataset}.transactions`\n  WHERE {window}\n  GROUP BY block_hash\n)")
        joins.append("LEFT JOIN tx_counts AS t USING (block_hash)")
        projections.append("COALESCE(r.receipt_count, 0) AS _receipt_count")
        checks.extend(("_receipt_count", feature.name, "transaction and receipt counts disagree") for feature in counts)
    if "receipts" in tables:
        weighted, statistics = "", ["COUNT(*) AS receipt_count"]
        if percentiles:
            weighted = (
                ", r.gas_used, b.gas_used AS block_gas_used,\n"
                "    r.effective_gas_price - b.base_fee_per_gas AS priority_fee,\n"
                "    SUM(r.gas_used) OVER (PARTITION BY b.block_number "
                "ORDER BY r.effective_gas_price - b.base_fee_per_gas, r.transaction_index "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_gas"
            )
            statistics.append("SUM(gas_used) AS receipt_gas_used")
            statistics.extend(
                f"IF(LOGICAL_OR(priority_fee IS NULL), NULL, MIN(IF(cumulative_gas >= DIV(block_gas_used * {percentile}, 100), priority_fee, NULL))) AS {name}"
                for name, percentile in percentiles
            )
            projections.extend(["b.gas_used AS _block_gas_used", "COALESCE(r.receipt_gas_used, 0) AS _receipt_gas_used"])
            checks.append(("_receipt_gas_used", "_block_gas_used", "receipts are incomplete"))
        fields = sorted(set(tables["receipts"]) - {"block_timestamp"})
        ctes.extend(
            [
                f"requested_receipts AS (\n  SELECT {', '.join(fields)}\n  FROM `{dataset}.receipts`\n  WHERE {window}\n)",
                f"block_receipts AS (\n  SELECT b.block_number{weighted}\n"
                "  FROM requested_receipts AS r JOIN requested_blocks AS b\n    ON r.block_hash = b.block_hash\n)",
                "receipt_stats AS (\n  SELECT block_number,\n    " + ",\n    ".join(statistics) + "\n  FROM block_receipts GROUP BY block_number\n)",
            ]
        )
        joins.append("LEFT JOIN receipt_stats AS r USING (block_number)")
    for feature in plan.acquired:
        recipe = feature.bigquery
        if isinstance(recipe, BlocksColumn):
            expression = f"UNIX_SECONDS(b.{recipe.column})" if recipe.type == "TIMESTAMP" else f"b.{recipe.column}"
        elif isinstance(recipe, TransactionRows):
            expression = "COALESCE(t.tx_count, 0)"
        elif isinstance(recipe, ReceiptPercentile):
            expression = f"IF(r.block_number IS NULL, 0, r.{feature.name})"
        else:
            raise ValueError(f"Feature {feature.name} is not available from BigQuery")
        projections.append(f"{expression} AS {feature.name}")
    result_schema = {projection.rpartition(" AS ")[2]: "INTEGER" for projection in projections}
    result_schema.update({feature.name: "STRING" for feature in plan.acquired if feature.dtype == "UTF-8"})
    sql = "WITH " + ",\n".join(ctes) + "\nSELECT\n  " + ",\n  ".join(projections) + "\nFROM requested_blocks AS b\n"
    if joins:
        sql += "\n".join(joins) + "\n"
    sql += "ORDER BY block_number"
    return BigQueryPlan(sql, tables, result_schema, tuple(checks))


def prepare_query(
    warehouse: BigQueryClient,
    dataset: str,
    query: BigQueryPlan,
    parameters: dict[str, int],
    maximum_bytes_billed: int,
) -> int:
    for table, expected in query.table_fields.items():
        actual = warehouse.table_schema(dataset, table)
        if any(field not in actual or not _compatible_bigquery_type(actual[field], dtype) for field, dtype in expected.items()):
            raise BlockweaverError("SOURCE_FEATURE_UNAVAILABLE", f"Configured BigQuery dataset does not support the selected features in {table}")
    bytes_processed, schema = warehouse.dry_run(query.sql, parameters)
    if set(schema) != set(query.result_schema) or any(not _compatible_bigquery_type(schema[name], dtype) for name, dtype in query.result_schema.items()):
        raise BlockweaverError("BIGQUERY_SCHEMA_INVALID", "BigQuery dry-run result schema does not match the selected feature plan")
    if bytes_processed > maximum_bytes_billed:
        raise BlockweaverError("BIGQUERY_COST_LIMIT", "BigQuery dry run exceeds maximum_bytes_billed")
    return bytes_processed


def _compatible_bigquery_type(actual: tuple[str, str], expected: str) -> bool:
    dtype, mode = actual
    return mode in {"NULLABLE", "REQUIRED"} and (dtype == expected or {dtype, expected} <= {"INTEGER", "INT64"})


def query_chunks(
    warehouse: BigQueryClient,
    query: BigQueryPlan,
    parameters: dict[str, int],
    maximum_bytes_billed: int,
    plan: Plan,
    chunk_size: int,
) -> Iterator[list[Row]]:
    rows: list[Row] = []
    expected = parameters["first_block"]
    for page in warehouse.pages(query.sql, parameters, maximum_bytes_billed, chunk_size):
        for value in page:
            rows.append(_parse_bigquery_row(value, query, plan, expected))
            expected += 1
            if len(rows) == chunk_size:
                yield rows
                rows = []
    if rows:
        yield rows


def _parse_bigquery_row(value: Mapping[str, object], query: BigQueryPlan, plan: Plan, expected: int) -> Row:
    if set(value) != set(query.result_schema):
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery returned a noncanonical row shape")
    if type(value["block_number"]) is not int or value["block_number"] != expected:
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery did not return the exact contiguous requested range")
    for witness, claim, problem in query.checks:
        if value[witness] != value[claim]:
            raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery {problem} for block {expected}")
    return plan.row(value)
