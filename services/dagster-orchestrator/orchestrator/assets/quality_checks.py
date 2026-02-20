"""Asset checks — GX-backed quality validation for each lakehouse layer."""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from orchestrator.resources.gx_resource import GXResource
from orchestrator.resources.iceberg import IcebergResource


@asset_check(asset="bronze_raw_trades", description="GX suite validation for bronze raw trades.")
def bronze_raw_trades_quality_check(iceberg: IcebergResource, gx: GXResource) -> AssetCheckResult:
    """Run the bronze_trades_suite against bronze.raw_trades."""
    import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401 — register custom expectations
    from data_quality.suites.bronze_suite import build_bronze_trades_suite

    try:
        table = iceberg.load_table("bronze.raw_trades")
        arrow = table.scan().to_arrow()
        df = arrow.to_pandas()
    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Could not load bronze.raw_trades: {exc}",
        )

    if len(df) == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            description="bronze.raw_trades is empty — no data to validate.",
        )

    suite = build_bronze_trades_suite()
    result = gx.validate(df, suite)

    failed = [r for r in result["results"] if not r["success"]]
    description = (
        f"GX bronze_trades_suite: {result['statistics'].get('successful_expectations', 0)}"
        f"/{result['statistics'].get('evaluated_expectations', 0)} expectations passed."
    )
    if failed:
        description += " Failed: " + ", ".join(r["expectation_type"] for r in failed)

    return AssetCheckResult(
        passed=result["success"],
        severity=AssetCheckSeverity.ERROR,
        description=description,
        metadata={"statistics": result["statistics"]},
    )


@asset_check(asset="silver_trades", description="GX suite validation for silver trades.")
def silver_trades_quality_check(iceberg: IcebergResource, gx: GXResource) -> AssetCheckResult:
    """Run the silver_trades_suite against silver.trades."""
    import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401
    from data_quality.suites.silver_suite import build_silver_trades_suite

    try:
        table = iceberg.load_table("silver.trades")
        arrow = table.scan().to_arrow()
        df = arrow.to_pandas()
    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Could not load silver.trades: {exc}",
        )

    if len(df) == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            description="silver.trades is empty — no data to validate.",
        )

    suite = build_silver_trades_suite()
    result = gx.validate(df, suite)

    failed = [r for r in result["results"] if not r["success"]]
    description = (
        f"GX silver_trades_suite: {result['statistics'].get('successful_expectations', 0)}"
        f"/{result['statistics'].get('evaluated_expectations', 0)} expectations passed."
    )
    if failed:
        description += " Failed: " + ", ".join(r["expectation_type"] for r in failed)

    return AssetCheckResult(
        passed=result["success"],
        severity=AssetCheckSeverity.ERROR,
        description=description,
        metadata={"statistics": result["statistics"]},
    )


@asset_check(
    asset="gold_daily_trading_summary",
    description="GX suite validation for gold daily trading summary.",
)
def gold_daily_summary_quality_check(iceberg: IcebergResource, gx: GXResource) -> AssetCheckResult:
    """Run the gold_daily_summary_suite against gold.daily_trading_summary."""
    import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401
    from data_quality.suites.gold_suite import build_gold_daily_summary_suite

    try:
        table = iceberg.load_table("gold.daily_trading_summary")
        arrow = table.scan().to_arrow()
        df = arrow.to_pandas()
    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Could not load gold.daily_trading_summary: {exc}",
        )

    if len(df) == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            description="gold.daily_trading_summary is empty — no data to validate.",
        )

    suite = build_gold_daily_summary_suite()
    result = gx.validate(df, suite)

    failed = [r for r in result["results"] if not r["success"]]
    description = (
        f"GX gold_daily_summary_suite: {result['statistics'].get('successful_expectations', 0)}"
        f"/{result['statistics'].get('evaluated_expectations', 0)} expectations passed."
    )
    if failed:
        description += " Failed: " + ", ".join(r["expectation_type"] for r in failed)

    return AssetCheckResult(
        passed=result["success"],
        severity=AssetCheckSeverity.ERROR,
        description=description,
        metadata={"statistics": result["statistics"]},
    )
