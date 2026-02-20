"""Data quality job — GX-backed validation across Bronze, Silver, and Gold layers."""

import logging

from dagster import In, Out, job, op

from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.gx_resource import GXResource
from orchestrator.resources.iceberg import IcebergResource

logger = logging.getLogger(__name__)


def _load_pandas(iceberg: IcebergResource, table_name: str):
    """Load an Iceberg table and return a Pandas DataFrame, or None if missing/empty."""
    try:
        table = iceberg.load_table(table_name)
        arrow = table.scan().to_arrow()
        if len(arrow) == 0:
            return None
        return arrow.to_pandas()
    except Exception:
        return None


@op(
    out={"result": Out(str)},
    description="Validate Bronze raw_trades with GX bronze_trades_suite.",
)
def validate_bronze(context, iceberg: IcebergResource, gx: GXResource) -> str:
    """Run GX bronze_trades_suite and log results."""
    import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401
    from data_quality.suites.bronze_suite import build_bronze_trades_suite

    df = _load_pandas(iceberg, "bronze.raw_trades")
    if df is None:
        context.log.info("DQ bronze: table missing or empty — skipping.")
        return "skipped"

    result = gx.validate(df, build_bronze_trades_suite())
    stats = result["statistics"]
    context.log.info(
        "DQ bronze_trades_suite: %s/%s expectations passed.",
        stats.get("successful_expectations", 0),
        stats.get("evaluated_expectations", 0),
    )
    if not result["success"]:
        failed = [r["expectation_type"] for r in result["results"] if not r["success"]]
        context.log.warning("DQ bronze FAILED expectations: %s", failed)
    return "passed" if result["success"] else "failed"


@op(
    ins={"upstream": In(str)},
    out={"result": Out(str)},
    description="Validate Silver trades with GX silver_trades_suite.",
)
def validate_silver(context, upstream: str, iceberg: IcebergResource, gx: GXResource) -> str:
    """Run GX silver_trades_suite and log results."""
    import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401
    from data_quality.suites.silver_suite import build_silver_trades_suite

    df = _load_pandas(iceberg, "silver.trades")
    if df is None:
        context.log.info("DQ silver: table missing or empty — skipping.")
        return "skipped"

    result = gx.validate(df, build_silver_trades_suite())
    stats = result["statistics"]
    context.log.info(
        "DQ silver_trades_suite: %s/%s expectations passed.",
        stats.get("successful_expectations", 0),
        stats.get("evaluated_expectations", 0),
    )
    if not result["success"]:
        failed = [r["expectation_type"] for r in result["results"] if not r["success"]]
        context.log.warning("DQ silver FAILED expectations: %s", failed)
    return "passed" if result["success"] else "failed"


@op(
    ins={"upstream": In(str)},
    out={"result": Out(str)},
    description="Validate Gold daily_trading_summary with GX gold_daily_summary_suite.",
)
def validate_gold(context, upstream: str, iceberg: IcebergResource, gx: GXResource) -> str:
    """Run GX gold_daily_summary_suite and log results."""
    import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401
    from data_quality.suites.gold_suite import build_gold_daily_summary_suite

    df = _load_pandas(iceberg, "gold.daily_trading_summary")
    if df is None:
        context.log.info("DQ gold: table missing or empty — skipping.")
        return "skipped"

    result = gx.validate(df, build_gold_daily_summary_suite())
    stats = result["statistics"]
    context.log.info(
        "DQ gold_daily_summary_suite: %s/%s expectations passed.",
        stats.get("successful_expectations", 0),
        stats.get("evaluated_expectations", 0),
    )
    if not result["success"]:
        failed = [r["expectation_type"] for r in result["results"] if not r["success"]]
        context.log.warning("DQ gold FAILED expectations: %s", failed)
    return "passed" if result["success"] else "failed"


@op(
    ins={"bronze_result": In(str), "silver_result": In(str), "gold_result": In(str)},
    description="Aggregate quality results and compute overall quality scores.",
)
def compute_quality_scores(
    context,
    bronze_result: str,
    silver_result: str,
    gold_result: str,
    iceberg: IcebergResource,
    duckdb_resource: DuckDBResource,
) -> None:
    """Compute weighted quality scores for each layer and log summary."""
    from data_quality.scoring import compute_quality_score

    layer_tables = {
        "bronze": ("bronze.raw_trades", "trade_id"),
        "silver": ("silver.trades", "trade_id"),
        "gold": ("gold.daily_trading_summary", "symbol"),
    }
    layer_results = {
        "bronze": bronze_result,
        "silver": silver_result,
        "gold": gold_result,
    }

    for layer, validation_result in layer_results.items():
        if validation_result == "skipped":
            context.log.info("Quality score [%s]: N/A (skipped)", layer)
            continue

        table_name, id_col = layer_tables[layer]
        df = _load_pandas(iceberg, table_name)
        if df is None:
            context.log.info("Quality score [%s]: N/A (empty)", layer)
            continue

        total_rows = len(df)
        null_count = int(df.isnull().any(axis=1).sum())
        dup_count = int(df.duplicated(subset=[id_col]).sum()) if id_col in df.columns else 0

        score = compute_quality_score(
            total_rows=total_rows,
            null_count=null_count,
            duplicate_count=dup_count,
            within_freshness_sla=(validation_result != "failed"),
        )
        context.log.info(
            "Quality score [%s]: overall=%.3f grade=%s "
            "(completeness=%.2f accuracy=%.2f freshness=%.2f uniqueness=%.2f)",
            layer,
            score.overall,
            score.grade,
            score.completeness,
            score.accuracy,
            score.freshness,
            score.uniqueness,
        )


@job(description="GX-backed data quality validation across Bronze, Silver, and Gold layers.")
def data_quality_job() -> None:
    """Run GX validation sequentially through each lakehouse layer."""
    bronze_result = validate_bronze()
    silver_result = validate_silver(upstream=bronze_result)
    gold_result = validate_gold(upstream=silver_result)
    compute_quality_scores(
        bronze_result=bronze_result,
        silver_result=silver_result,
        gold_result=gold_result,
    )
