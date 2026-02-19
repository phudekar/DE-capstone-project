# 11. Scalability & Performance

> Implementation plan for partitioning, bucketing, caching, query optimization, and cost-based optimization across the stock market trade data pipeline (Kafka, Flink, Iceberg/Parquet, DuckDB).

---

## Table of Contents

1. [Partitioning Strategies](#1-partitioning-strategies)
2. [Bucketing and Clustering](#2-bucketing-and-clustering)
3. [Caching Layers](#3-caching-layers)
4. [Query Optimization](#4-query-optimization)
5. [Cost-Based Optimization](#5-cost-based-optimization)
6. [Write Performance](#6-write-performance)
7. [Read Performance](#7-read-performance)
8. [Compaction and Maintenance](#8-compaction-and-maintenance)
9. [Benchmarking](#9-benchmarking)
10. [Implementation Steps](#10-implementation-steps)
11. [Testing Strategy](#11-testing-strategy)

---

## 1. Partitioning Strategies

Partitioning is the single most impactful decision for query performance and storage efficiency. The strategy must align with how data is produced (streaming trades) and how it is consumed (time-range + symbol queries, aggregations, dashboards).

### 1.1 Time-Based Partitioning

#### Fact Tables (trades, orderbook, marketdata)

| Property | Value | Rationale |
|---|---|---|
| Primary partition | `days(timestamp)` | Aligns with trading days; most queries filter by date range |
| Partition granularity | Daily | One partition per trading day; ~252 partitions/year |
| Hidden partitioning | Yes (Iceberg) | No extra `date` column needed; timestamp is transformed at write time |
| Partition evolution | Supported | Can evolve to `hours(timestamp)` without rewriting existing data |

**Why daily partitions:**

- Stock markets operate on a daily cycle. Queries almost always include a date predicate (`WHERE trade_date = '2024-01-15'` or `WHERE trade_date BETWEEN ...`).
- Daily granularity produces manageable partition counts: ~252/year vs ~60,480 for minute-level.
- Each daily partition holds enough data to produce well-sized Parquet files (target 128--512 MB).
- Iceberg hidden partitioning means the physical partition layout is invisible to query authors -- they filter on `timestamp` and Iceberg automatically prunes to the correct day.

**Partition evolution path:**

```
-- Initial: daily partitions
ALTER TABLE silver.trades
  ADD PARTITION FIELD days(timestamp);

-- Future: if intraday latency demands it, evolve to hourly
-- Existing daily files remain untouched; new writes use hourly
ALTER TABLE silver.trades
  ADD PARTITION FIELD hours(timestamp);
```

Iceberg handles mixed partition layouts transparently. Old daily files coexist with new hourly files, and the query planner prunes across both.

#### Gold Aggregation Tables

| Table | Partition | Rationale |
|---|---|---|
| `gold.daily_symbol_summary` | `months(date)` | Queried over longer horizons (month/quarter/year) |
| `gold.trader_performance` | `months(period_end)` | Monthly rollups; low volume per month |
| `gold.market_overview` | `days(snapshot_date)` | Daily snapshots; moderate volume |

Monthly partitions for gold tables keep partition counts low (12/year) while still enabling efficient range pruning for quarterly and yearly dashboards.

### 1.2 Hash-Based Partitioning

#### Kafka Topics

```
Topic: raw.trades         -> 12 partitions, key = symbol
Topic: raw.orderbook      -> 12 partitions, key = symbol
Topic: raw.marketdata      -> 12 partitions, key = symbol
Topic: processed.trades    -> 12 partitions, key = symbol
Topic: alerts.anomalies    ->  4 partitions, key = symbol
```

**Design decisions:**

| Decision | Value | Rationale |
|---|---|---|
| Partition key | `symbol` hash | Guarantees per-symbol ordering (critical for trade sequencing) |
| Partition count | 12 | Accommodates up to 12 Flink consumer instances; divisible by common cluster sizes (2, 3, 4, 6) |
| Partitioner | Default (murmur2) | Even distribution across partitions; standard Kafka behavior |
| Rebalance impact | Minimal | Symbol-level ordering preserved through cooperative sticky assignment |

**Why 12 partitions:**

- Parallelism ceiling: Each partition can be consumed by at most one consumer in a consumer group. 12 partitions allow scaling to 12 parallel Flink subtasks.
- Even distribution: With hundreds of symbols hashed across 12 buckets, distribution is statistically even. If a few high-volume symbols cause skew, consider increasing to 24 or 48.
- Overhead: Each partition costs ~10 KB of metadata on the broker. 12 partitions per topic is negligible.

#### Iceberg Bucket Partitioning

```sql
-- Dimension tables: bucket by symbol for fast lookups
ALTER TABLE silver.dim_symbols
  ADD PARTITION FIELD bucket(16, symbol);

-- Enables predicate pushdown: WHERE symbol = 'AAPL' reads only 1/16 of files
```

Bucket count of 16 is chosen because:
- 16 is a power of 2 (efficient hash distribution).
- With ~500 symbols across 16 buckets, each bucket holds ~31 symbols -- enough for well-sized files without excessive fragmentation.

### 1.3 Composite Partitioning

For high-volume fact tables that are queried by both time range and symbol:

```sql
-- Silver trades: composite partition
ALTER TABLE silver.fact_trades
  ADD PARTITION FIELD days(timestamp);
ALTER TABLE silver.fact_trades
  ADD PARTITION FIELD bucket(8, symbol);
```

**Resulting layout:**

```
silver/fact_trades/
  timestamp_day=2024-01-15/
    symbol_bucket=0/    -> ~60 symbols, data files
    symbol_bucket=1/    -> ~60 symbols, data files
    ...
    symbol_bucket=7/    -> ~60 symbols, data files
  timestamp_day=2024-01-16/
    symbol_bucket=0/
    ...
```

**Query patterns served efficiently:**

| Query Pattern | Partitions Read |
|---|---|
| `WHERE timestamp = '2024-01-15 10:30:00'` | 1 day x 8 buckets = 8 partitions |
| `WHERE symbol = 'AAPL'` | All days x 1 bucket = N partitions |
| `WHERE timestamp BETWEEN ... AND symbol = 'AAPL'` | K days x 1 bucket = K partitions (optimal) |

8 symbol buckets (instead of 16) keep the file count manageable: 252 days x 8 buckets = 2,016 partition directories per year.

---

## 2. Bucketing and Clustering

Bucketing and clustering control the physical layout of data within partitions to maximize scan efficiency.

### 2.1 Iceberg Sort Orders

Sort orders define how rows are arranged within each data file. Well-chosen sort orders turn full-file scans into partial-file scans via min/max statistics.

```sql
-- fact_trades: sort by symbol then timestamp within each partition
ALTER TABLE silver.fact_trades
  WRITE ORDERED BY symbol ASC, timestamp ASC;

-- fact_orderbook: sort by symbol, level, timestamp
ALTER TABLE silver.fact_orderbook
  WRITE ORDERED BY symbol ASC, level ASC, timestamp ASC;

-- dim_symbols: sort by symbol (primary lookup key)
ALTER TABLE silver.dim_symbols
  WRITE ORDERED BY symbol ASC;
```

**Why (symbol, timestamp) for fact_trades:**

- Most analytical queries filter on symbol and then scan a time range: `WHERE symbol = 'AAPL' AND timestamp BETWEEN ...`
- Sorting by symbol first means all AAPL rows are contiguous. Row group min/max statistics on `symbol` allow skipping entire row groups.
- Secondary sort on timestamp enables efficient range scans within a symbol's data.

### 2.2 Z-Order Optimization

For multi-dimensional analytical queries that filter on combinations of columns without a dominant access pattern:

```sql
-- Z-order on (symbol, trade_type) for analytical queries
-- that filter on either or both dimensions
ALTER TABLE gold.trade_analytics
  WRITE DISTRIBUTED BY PARTITION
  LOCALLY ORDERED BY zorder(symbol, trade_type);
```

**When to use Z-order vs linear sort:**

| Scenario | Strategy |
|---|---|
| Single dominant filter column | Linear sort on that column |
| Two equally important filter columns | Z-order on both |
| Range scan on one column | Linear sort (Z-order fragments ranges) |
| Point lookups on multiple columns | Z-order |

For this project, Z-order is primarily useful on gold analytical tables where queries mix symbol, trade_type, and exchange filters unpredictably. Silver fact tables use linear sort because the (symbol, timestamp) access pattern is dominant.

### 2.3 File Size Targets

| Parameter | Target | Rationale |
|---|---|---|
| Target file size | 256 MB | Balance between parallelism (more files) and overhead (fewer files) |
| Minimum file size | 128 MB | Files below this are candidates for compaction |
| Maximum file size | 512 MB | Files above this should be split |
| Row group size | 128 MB | Standard Parquet row group; enables row-group-level predicate pushdown |
| Page size | 1 MB | Standard Parquet page; unit of decompression |

**File size rationale:**

- Too small (< 64 MB): Excessive file listing overhead, metadata bloat, poor compression.
- Too large (> 1 GB): Poor parallelism, wasted reads when only partial data needed, high memory during writes.
- 256 MB is the Iceberg default and a well-tested sweet spot for analytical workloads.

### 2.4 Iceberg Configuration

```properties
# iceberg-defaults.properties

# File sizing
write.target-file-size-bytes=268435456     # 256 MB
write.parquet.row-group-size-bytes=134217728  # 128 MB
write.parquet.page-size-bytes=1048576         # 1 MB

# Sort
write.distribution-mode=hash                  # Distribute by partition before sort
write.wap.enabled=true                         # Write-Audit-Publish for safe commits

# Metadata
write.metadata.delete-after-commit.enabled=true
write.metadata.previous-versions-max=10
```

---

## 3. Caching Layers

Caching reduces repeated computation and I/O. The trade data pipeline has three distinct caching layers, each addressing a different access pattern.

### 3.1 Application-Level Caching (Redis)

```
Architecture:
  API Server -> Redis Cache -> DuckDB / Iceberg
                  |
                  v
              Cache Hit? -> Return cached result
              Cache Miss? -> Query backend, populate cache, return
```

#### Cache Categories

| Cache Key Pattern | Data | TTL | Invalidation |
|---|---|---|---|
| `ref:symbol:{symbol}` | Symbol reference data (name, exchange, sector) | 1 hour | Event-driven on dim_symbols update |
| `market:overview` | Market-wide summary (indices, volume, top movers) | 1 minute | Time-based; refreshed by streaming pipeline |
| `query:{hash}` | Popular query results (top N, aggregations) | 5 minutes | Time-based |
| `rate:{client_id}:{window}` | API rate limit counters | 1 minute | Automatic expiry |
| `session:{token}` | User session data | 30 minutes | Explicit logout or expiry |

#### Redis Configuration

```yaml
# redis/redis.conf
maxmemory 512mb
maxmemory-policy allkeys-lru    # Evict least recently used when full
save 60 1000                     # RDB snapshot every 60s if 1000+ writes
appendonly no                    # No AOF; cache data is reconstructible
tcp-keepalive 300
```

#### Cache Invalidation Strategy

```python
# services/api/cache/invalidation.py

class CacheInvalidator:
    """
    Two invalidation modes:
    1. Time-based TTL: Most data expires naturally.
    2. Event-driven: Dimension changes trigger immediate invalidation.
    """

    def on_dimension_update(self, table: str, key: str):
        """Called by Flink when a dimension record changes (SCD Type 1/2)."""
        if table == "dim_symbols":
            self.redis.delete(f"ref:symbol:{key}")
            self.redis.delete("market:overview")  # May reference symbol data

    def on_new_trade_batch(self):
        """Called after each micro-batch of trades is committed to Iceberg."""
        # Invalidate market overview (will be refreshed on next request)
        self.redis.delete("market:overview")
        # Do NOT invalidate query caches; let TTL handle staleness
```

### 3.2 Query-Level Caching

#### DuckDB Result Caching

```sql
-- DuckDB materialized views for frequently accessed aggregations
CREATE OR REPLACE TABLE cache.daily_volume AS
SELECT
    symbol,
    DATE_TRUNC('day', timestamp) AS trade_date,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_volume,
    SUM(price * quantity) AS total_value,
    MIN(price) AS low,
    MAX(price) AS high,
    FIRST(price ORDER BY timestamp) AS open,
    LAST(price ORDER BY timestamp) AS close
FROM iceberg_scan('silver.fact_trades')
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY symbol, DATE_TRUNC('day', timestamp);

-- Refresh strategy: Dagster asset triggered after each Iceberg commit
-- This table lives in DuckDB memory/local storage for sub-second queries
```

#### Superset Query Cache

```python
# superset/superset_config.py

# Redis-backed query cache
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,       # 5 minutes
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://redis:6379/1",
}

# Data cache for query results
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,       # 10 minutes
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": "redis://redis:6379/2",
}

# Thumbnail cache for dashboard screenshots
THUMBNAIL_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,     # 24 hours
    "CACHE_KEY_PREFIX": "superset_thumb_",
    "CACHE_REDIS_URL": "redis://redis:6379/3",
}
```

### 3.3 Data-Level Caching

#### Iceberg Metadata Caching

```properties
# Iceberg catalog cache configuration
# Caches table metadata (schema, partitions, snapshots) in memory

catalog.cache-enabled=true
catalog.cache.expiration-interval-ms=300000   # 5 minutes
```

The Iceberg catalog cache avoids repeated reads of metadata files (manifest lists, manifests) for every query. On a local file system catalog, this eliminates repeated JSON/Avro deserialization.

#### Parquet Footer Caching

```python
# DuckDB configuration for Parquet footer caching
# Parquet footers contain schema, row group metadata, column statistics
# Caching them avoids re-reading the tail of every file on each query

duckdb.execute("SET enable_object_cache=true;")
# DuckDB caches Parquet metadata in its buffer pool
```

#### OS Page Cache

On the local deployment, the OS page cache is the most important "free" cache:

- Frequently accessed Parquet files stay in RAM via the kernel page cache.
- No configuration needed, but leave headroom: if the machine has 32 GB RAM, do not allocate more than 20 GB to application processes.
- Monitor cache hit rates via `vmstat` or `sar`.

```bash
# Monitor page cache effectiveness
# "cache" column shows OS page cache size
free -h

# Detailed cache statistics
vmstat 1 5
```

---

## 4. Query Optimization

### 4.1 Predicate Pushdown

Predicate pushdown means pushing WHERE-clause filters as deep as possible into the storage layer, so irrelevant data is never read from disk.

#### Layer 1: Iceberg Partition Pruning

```
Query: SELECT * FROM fact_trades
       WHERE timestamp BETWEEN '2024-01-15' AND '2024-01-17'
         AND symbol = 'AAPL'

Iceberg scan planning:
  1. Read manifest list -> identifies 365 manifests (one per day)
  2. Partition pruning -> selects 3 manifests (Jan 15, 16, 17)
  3. Within those manifests, bucket pruning -> selects 1 bucket (AAPL's bucket)
  4. Result: Read ~3 data files instead of ~2,920

Files scanned: 3 / 2,920 = 0.1% of total data
```

#### Layer 2: Parquet Row Group Pruning

```
Within each selected data file:
  1. Read Parquet footer (column statistics per row group)
  2. Row group has min_symbol='AACG', max_symbol='ABNB'
     -> Symbol 'AAPL' falls in range -> read this row group
  3. Row group has min_symbol='MSFT', max_symbol='NFLX'
     -> Symbol 'AAPL' not in range -> SKIP this row group

Additional rows skipped: ~75% of rows within selected files
```

#### Layer 3: Parquet Page-Level Filtering (Column Index)

```
Within each selected row group:
  1. Read column index for 'symbol' column
  2. Page 0: min='AACG', max='ADBE' -> skip
  3. Page 1: min='AAPL', max='AAPL' -> READ
  4. Page 2: min='AMZN', max='ABNB' -> skip

Additional rows skipped: ~90% of rows within selected row groups
```

#### Implementation Checklist

```
[x] Ensure all fact table queries include partition column (timestamp) in WHERE clause
[x] Add symbol to WHERE clause for symbol-specific queries (triggers bucket pruning)
[x] Verify Parquet files have column statistics enabled (write.parquet.column-stats=true)
[x] Verify Parquet page-level column index is enabled
[x] Test with EXPLAIN ANALYZE to confirm pushdown is happening
```

### 4.2 Partition Pruning Metrics

Track how effective partition pruning is for every query:

```sql
-- DuckDB: Examine query plan to verify pruning
EXPLAIN ANALYZE
SELECT symbol, COUNT(*), AVG(price)
FROM iceberg_scan('silver.fact_trades')
WHERE timestamp BETWEEN '2024-01-15' AND '2024-01-17'
  AND symbol = 'AAPL'
GROUP BY symbol;

-- Look for:
--   "Files scanned: 3"
--   "Files skipped by partition filter: 2917"
--   "Row groups scanned: 2"
--   "Row groups skipped by statistics: 10"
```

#### Monitoring Dashboard Metrics

| Metric | Source | Alert Threshold |
|---|---|---|
| `iceberg.scan.files_scanned` | Iceberg metrics reporter | > 100 files for a single-day query |
| `iceberg.scan.files_total` | Iceberg metrics reporter | Informational |
| `iceberg.scan.planning_time_ms` | Iceberg metrics reporter | > 5000 ms |
| `query.execution_time_ms` | DuckDB / API layer | Varies by query type |
| `query.rows_scanned / rows_returned` | DuckDB | Ratio > 1000:1 indicates missing pushdown |

### 4.3 Column Pruning

Columnar storage (Parquet) means only requested columns are read from disk. This is a major advantage over row-oriented formats.

```sql
-- BAD: Reads all columns from disk
SELECT * FROM silver.fact_trades WHERE symbol = 'AAPL';

-- GOOD: Reads only 3 columns from disk
SELECT symbol, price, quantity FROM silver.fact_trades WHERE symbol = 'AAPL';
```

**Implementation across the stack:**

| Layer | Action |
|---|---|
| GraphQL API | Resolver introspects requested fields; only passes those to DuckDB query |
| DuckDB | Automatic column pruning; only reads requested columns from Parquet |
| Flink | Use explicit column lists in SELECT statements, not `SELECT *` |
| Superset | Define virtual datasets with only the columns needed for each dashboard |

```python
# services/api/resolvers/trades.py

def resolve_trades(info, symbol: str, date: str):
    # Extract only the fields requested by the GraphQL client
    requested_fields = get_requested_fields(info)
    columns = ", ".join(requested_fields)

    query = f"""
        SELECT {columns}
        FROM iceberg_scan('silver.fact_trades')
        WHERE symbol = ? AND DATE_TRUNC('day', timestamp) = ?
    """
    return duckdb.execute(query, [symbol, date]).fetchall()
```

### 4.4 Join Optimization

| Join Type | When to Use | Example |
|---|---|---|
| Broadcast join | Small dim table (< 10 MB) joined to large fact table | `fact_trades JOIN dim_symbols` |
| Sort-merge join | Two large sorted tables joined on sort key | `fact_trades JOIN fact_orderbook ON (symbol, timestamp)` |
| Hash join | Medium tables without sort alignment | `fact_trades JOIN dim_traders` |
| Pre-aggregation | Reduce cardinality before joining | Aggregate trades to daily level, then join to daily benchmarks |

```sql
-- Broadcast join: DuckDB automatically broadcasts small tables
-- Ensure dimension tables are small enough to fit in memory
SELECT t.symbol, t.price, t.quantity, s.company_name, s.sector
FROM silver.fact_trades t
JOIN silver.dim_symbols s ON t.symbol = s.symbol
WHERE t.timestamp BETWEEN '2024-01-15' AND '2024-01-17';

-- Pre-aggregation before join (reduce rows from millions to thousands)
WITH daily_trades AS (
    SELECT symbol, DATE_TRUNC('day', timestamp) AS trade_date,
           SUM(quantity) AS total_volume, AVG(price) AS avg_price
    FROM silver.fact_trades
    WHERE timestamp BETWEEN '2024-01-01' AND '2024-03-31'
    GROUP BY symbol, DATE_TRUNC('day', timestamp)
)
SELECT dt.*, b.benchmark_price
FROM daily_trades dt
JOIN gold.daily_benchmarks b ON dt.symbol = b.symbol AND dt.trade_date = b.date;
```

---

## 5. Cost-Based Optimization

Cost-based optimization (CBO) uses table statistics to choose the best execution plan for a query. Without statistics, the optimizer guesses -- and often guesses wrong.

### 5.1 DuckDB Automatic CBO

DuckDB has a built-in cost-based optimizer that automatically:
- Chooses join order based on estimated cardinalities
- Selects join algorithms (hash, merge, nested loop)
- Decides on aggregation strategies (hash vs sort-based)
- Optimizes filter ordering

```sql
-- Verify DuckDB is using CBO effectively
EXPLAIN ANALYZE
SELECT t.symbol, s.sector, COUNT(*) AS trade_count
FROM silver.fact_trades t
JOIN silver.dim_symbols s ON t.symbol = s.symbol
WHERE t.timestamp BETWEEN '2024-01-15' AND '2024-01-17'
GROUP BY t.symbol, s.sector
ORDER BY trade_count DESC
LIMIT 20;

-- Look for:
--   Estimated cardinality at each node
--   Join order (small table should be build side of hash join)
--   Filter pushdown before join
```

### 5.2 Table Statistics Maintenance

Accurate statistics are the foundation of CBO. Stale statistics lead to bad plans.

```sql
-- DuckDB: Statistics are computed automatically during query execution
-- Force statistics collection for Iceberg tables by running:
ANALYZE iceberg_scan('silver.fact_trades');

-- View statistics
SELECT * FROM duckdb_statistics('silver.fact_trades');
```

#### Statistics to Maintain

| Statistic | Purpose | Update Frequency |
|---|---|---|
| Row count per partition | Join cardinality estimation | After each write |
| Column cardinality (NDV) | Join selectivity estimation | Daily |
| Null percentage | Filter selectivity | Daily |
| Min/max values per column | Range filter estimation | After each write (automatic in Parquet) |
| Histogram (equi-depth) | Skew-aware estimation | Weekly |

### 5.3 Iceberg Table Statistics

Iceberg maintains statistics at multiple levels, all used for query planning:

```
Manifest List (snapshot)
  -> Row count: 10,000,000
  -> File count: 40

Manifest File (per partition range)
  -> Partition: timestamp_day=2024-01-15
  -> Row count: 40,000
  -> File count: 4
  -> Column stats per file:
       symbol: min='AACG', max='ZTS', null_count=0, nan_count=0
       price:  min=0.50, max=4999.99, null_count=0
       ...

Data File (Parquet footer)
  -> Row group stats (same as above, per row group)
  -> Page-level column index
```

```python
# Inspect Iceberg table statistics programmatically
# scripts/inspect_statistics.py

import pyiceberg

table = catalog.load_table("silver.fact_trades")

# Snapshot-level stats
snapshot = table.current_snapshot()
print(f"Total records: {snapshot.summary['total-records']}")
print(f"Total files: {snapshot.summary['total-data-files']}")
print(f"Total size: {snapshot.summary['total-files-size']} bytes")

# Manifest-level stats
for manifest in snapshot.manifests(table.io):
    print(f"  Partition: {manifest.partition_spec_id}")
    print(f"  Files: {manifest.existing_files_count}")
    print(f"  Rows: {manifest.existing_rows_count}")
```

### 5.4 Query Plan Analysis

```sql
-- DuckDB: Full query plan with execution statistics
EXPLAIN ANALYZE
SELECT symbol, DATE_TRUNC('hour', timestamp) AS hour,
       COUNT(*) AS trades, SUM(quantity * price) AS volume
FROM iceberg_scan('silver.fact_trades')
WHERE timestamp BETWEEN '2024-01-15' AND '2024-01-15 23:59:59'
GROUP BY symbol, DATE_TRUNC('hour', timestamp)
HAVING SUM(quantity * price) > 1000000
ORDER BY volume DESC;

-- Output includes:
-- 1. Logical plan (what operations)
-- 2. Physical plan (how operations execute)
-- 3. Per-operator timing and row counts
-- 4. Memory usage per operator
```

```java
// Flink: Execution plan analysis
// flink-jobs/src/main/java/com/pipeline/PlanAnalyzer.java

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// ... define pipeline ...

// Print execution plan as JSON
String plan = env.getExecutionPlan();
System.out.println(plan);

// Visualize at: https://flink.apache.org/visualizer/
```

---

## 6. Write Performance

### 6.1 Kafka Producer Optimization

```properties
# kafka/producer.properties

# Batching: Accumulate messages before sending
linger.ms=5                          # Wait up to 5ms to batch messages
batch.size=32768                     # 32 KB batch size per partition
buffer.memory=67108864               # 64 MB total buffer

# Compression: Reduce network and disk I/O
compression.type=snappy              # Fast compression; ~2:1 ratio for trade data

# Reliability
acks=1                               # Leader ack (trade-off: speed vs durability)
retries=3                            # Retry on transient failures
retry.backoff.ms=100

# Throughput
max.in.flight.requests.per.connection=5   # Pipeline requests
send.buffer.bytes=131072              # 128 KB socket send buffer
```

**Why these values:**

| Setting | Value | Rationale |
|---|---|---|
| `linger.ms=5` | 5 ms | Allows batching without perceptible latency. At 1000 trades/sec, this batches ~5 trades per send, reducing network calls by 5x. |
| `batch.size=32768` | 32 KB | Sufficient for typical trade message sizes (~200 bytes each). Holds ~160 messages per batch. |
| `compression.type=snappy` | snappy | 2--3x faster than gzip with 60--70% of the compression ratio. Trade data has repetitive fields (symbol, exchange) that compress well. |
| `acks=1` | Leader ack | For trade data, leader ack is sufficient. Full ISR ack (`acks=all`) adds ~5 ms latency per batch. If durability is critical, switch to `acks=all`. |

### 6.2 Flink Write Optimization

```java
// flink-jobs/src/main/java/com/pipeline/IcebergSinkConfig.java

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(false)
    .distributionMode(DistributionMode.HASH)     // Hash by partition key
    .writeParallelism(4)                          // Match Kafka partition count / 3
    .upsert(false)                                // Append-only for fact tables
    .set("write.target-file-size-bytes", "268435456")  // 256 MB
    .set("write.parquet.compression-codec", "zstd")    // Better ratio than snappy for cold storage
    .set("write.parquet.row-group-size-bytes", "134217728")  // 128 MB
    .append();
```

### 6.3 Iceberg Write Optimization

```properties
# Commit configuration
commit.retry.num-retries=4
commit.retry.min-wait-ms=100
commit.retry.max-wait-ms=60000

# Concurrent writes
write.wap.enabled=true                # Write-Audit-Publish pattern
write.distribution-mode=hash          # Hash-distribute by partition before write

# Target file sizing
write.target-file-size-bytes=268435456   # 256 MB
```

### 6.4 Parquet Encoding Optimization

Different column types benefit from different encoding strategies:

| Column | Data Type | Encoding | Rationale |
|---|---|---|---|
| `symbol` | STRING | DICTIONARY | Low cardinality (~500 unique values); dictionary encoding stores each value once |
| `exchange` | STRING | DICTIONARY | Very low cardinality (~5 values) |
| `trade_type` | STRING | DICTIONARY | Very low cardinality (BUY/SELL) |
| `timestamp` | TIMESTAMP | DELTA_BINARY_PACKED | Monotonically increasing; delta encoding stores differences (very small integers) |
| `price` | DECIMAL | PLAIN | High cardinality; dictionary would be larger than raw data |
| `quantity` | INTEGER | DELTA_BINARY_PACKED | Often similar values; delta encoding effective |
| `trade_id` | STRING (UUID) | PLAIN | Unique per row; no encoding helps |

```python
# Parquet write configuration
# Applied via Iceberg table properties

table_properties = {
    "write.parquet.compression-codec": "zstd",
    "write.parquet.compression-level": "3",          # zstd level 3: fast + good ratio
    "write.parquet.dict-size-bytes": "2097152",      # 2 MB dictionary page
    "write.parquet.page-size-bytes": "1048576",      # 1 MB data page
    "write.parquet.row-group-size-bytes": "134217728",  # 128 MB row group
    "write.parquet.bloom-filter-enabled.column.symbol": "true",  # Bloom filter for symbol lookups
    "write.parquet.bloom-filter-fpp.column.symbol": "0.01",      # 1% false positive rate
}
```

---

## 7. Read Performance

### 7.1 Iceberg Table Scan Optimization

```properties
# Scan planning configuration
read.split.target-size=134217728     # 128 MB split size (one split = one task)
read.split.metadata-target-size=33554432  # 32 MB metadata split
read.split.planning-lookback=10      # Look back 10 splits for bin-packing
read.split.open-file-cost=4194304    # 4 MB overhead per file (for bin-packing)
```

**Split size rationale:**
- Each split becomes one read task. 128 MB splits mean each task processes ~128 MB of data.
- Too small: Task scheduling overhead dominates.
- Too large: Poor parallelism; one slow task delays the entire query.
- 128 MB is a good default that produces reasonable task counts (~40 tasks for 5 GB of data).

### 7.2 DuckDB Vectorized Execution

DuckDB processes data in vectors (batches of 2048 rows) rather than row-by-row. This is automatic, but configuration affects performance:

```sql
-- DuckDB performance settings
SET threads = 4;                     -- Match available CPU cores
SET memory_limit = '4GB';            -- Working memory for query execution
SET temp_directory = '/tmp/duckdb';  -- Spill location for large queries

-- Enable parallel reads
SET enable_progress_bar = true;      -- Visual feedback for long queries
SET enable_object_cache = true;      -- Cache Parquet metadata
SET force_parallelism = false;       -- Let DuckDB decide parallelism

-- Iceberg-specific
SET iceberg_scan_parallel_manifest_reads = true;
```

### 7.3 Parallel File Reading

```python
# services/api/query/parallel_reader.py

import duckdb
from concurrent.futures import ThreadPoolExecutor

class ParallelIcebergReader:
    """
    For very large scans, split across multiple DuckDB connections.
    Each connection reads a subset of files in parallel.
    """

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers

    def read_partitioned(self, table: str, partitions: list[str]) -> list:
        """Read multiple partitions in parallel, each on its own DuckDB connection."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._read_partition, table, partition)
                for partition in partitions
            ]
            results = [f.result() for f in futures]
        return self._merge_results(results)

    def _read_partition(self, table: str, partition: str):
        # Each thread gets its own DuckDB connection
        conn = duckdb.connect()
        return conn.execute(
            f"SELECT * FROM iceberg_scan('{table}') WHERE {partition}"
        ).fetchall()
```

### 7.4 Projection Pushdown

Projection pushdown ensures only needed columns are read from Parquet files. This is distinct from predicate pushdown (which filters rows).

```
Query: SELECT symbol, price FROM fact_trades WHERE timestamp = '2024-01-15'

Without projection pushdown:
  Read all 15 columns from Parquet -> Filter rows -> Project 2 columns
  I/O: 100 MB

With projection pushdown:
  Read only 2 columns (symbol, price) from Parquet -> Filter rows
  I/O: 15 MB (85% reduction)
```

DuckDB and Flink both support projection pushdown automatically when reading Parquet/Iceberg. The key implementation requirement is to never use `SELECT *` in production queries.

---

## 8. Compaction and Maintenance

Streaming workloads produce many small files. Without compaction, query performance degrades over time as the planner must open and read metadata for thousands of tiny files.

### 8.1 Small File Compaction

```python
# dagster/assets/maintenance/compaction.py

from dagster import asset, schedule, RunRequest

COMPACTION_CONFIG = {
    "max_files_per_partition": 50,      # Trigger compaction above this
    "target_file_size_bytes": 268435456,  # 256 MB
    "min_input_files": 5,               # Don't compact fewer than 5 files
    "partial_progress_enabled": True,    # Commit partial progress on failure
    "max_concurrent_file_group_rewrites": 4,
}

@asset(
    description="Compact small files in silver.fact_trades",
    group_name="maintenance",
)
def compact_fact_trades(context):
    """
    Merge small files into target-sized files within each partition.
    Runs daily during off-hours (after market close).
    """
    table = catalog.load_table("silver.fact_trades")

    result = table.rewrite_data_files(
        target_size_in_bytes=COMPACTION_CONFIG["target_file_size_bytes"],
        min_input_files=COMPACTION_CONFIG["min_input_files"],
        max_concurrent_file_group_rewrites=COMPACTION_CONFIG["max_concurrent_file_group_rewrites"],
        partial_progress_enabled=COMPACTION_CONFIG["partial_progress_enabled"],
    )

    context.log.info(
        f"Compaction complete: "
        f"rewritten={result.rewritten_data_files_count}, "
        f"added={result.added_data_files_count}, "
        f"bytes_before={result.rewritten_bytes_count}, "
        f"bytes_after={result.added_bytes_count}"
    )
    return result

@schedule(
    cron_schedule="0 2 * * *",  # 2:00 AM daily
    job_name="compaction_job",
    execution_timezone="America/New_York",
)
def daily_compaction_schedule(context):
    return RunRequest()
```

### 8.2 Snapshot Management

```python
# dagster/assets/maintenance/snapshot_management.py

@asset(
    description="Expire old snapshots to reclaim storage",
    group_name="maintenance",
)
def expire_snapshots(context):
    """
    Expire snapshots older than 7 days, retaining at least 3 for time-travel.
    """
    import time

    tables = [
        "silver.fact_trades",
        "silver.fact_orderbook",
        "silver.fact_marketdata",
        "gold.daily_symbol_summary",
        "gold.trader_performance",
    ]

    seven_days_ago_ms = int((time.time() - 7 * 86400) * 1000)

    for table_name in tables:
        table = catalog.load_table(table_name)

        result = table.expire_snapshots(
            older_than_ms=seven_days_ago_ms,
            retain_last=3,  # Always keep at least 3 snapshots
        )

        context.log.info(
            f"{table_name}: expired {result.deleted_snapshots_count} snapshots, "
            f"deleted {result.deleted_data_files_count} data files, "
            f"freed {result.deleted_bytes_count} bytes"
        )
```

### 8.3 Orphan File Cleanup

Orphan files are data files not referenced by any snapshot. They can accumulate from failed commits or incomplete compactions.

```python
# dagster/assets/maintenance/orphan_cleanup.py

@asset(
    description="Remove orphan files not referenced by any snapshot",
    group_name="maintenance",
)
def cleanup_orphan_files(context):
    """
    Weekly cleanup of orphan files older than 3 days.
    The 3-day buffer ensures we don't delete files from in-progress commits.
    """
    import time

    three_days_ago_ms = int((time.time() - 3 * 86400) * 1000)

    tables = ["silver.fact_trades", "silver.fact_orderbook", "silver.fact_marketdata"]

    for table_name in tables:
        table = catalog.load_table(table_name)

        result = table.remove_orphan_files(
            older_than_ms=three_days_ago_ms,
            dry_run=False,  # Set True for first run to preview
        )

        context.log.info(
            f"{table_name}: removed {result.orphan_file_count} orphan files, "
            f"freed {result.orphan_file_bytes} bytes"
        )
```

### 8.4 Manifest Rewrite

```python
# dagster/assets/maintenance/manifest_rewrite.py

@asset(
    description="Rewrite manifests when they become too large or numerous",
    group_name="maintenance",
)
def rewrite_manifests(context):
    """
    Rewrite manifests when:
    - Individual manifest exceeds 8 MB
    - Total manifest count exceeds 100
    Combines small manifests and splits large ones.
    """
    tables = ["silver.fact_trades", "silver.fact_orderbook"]

    for table_name in tables:
        table = catalog.load_table(table_name)

        current_snapshot = table.current_snapshot()
        manifests = current_snapshot.manifests(table.io)

        total_manifests = len(manifests)
        large_manifests = sum(1 for m in manifests if m.manifest_length > 8 * 1024 * 1024)

        if total_manifests > 100 or large_manifests > 0:
            result = table.rewrite_manifests()
            context.log.info(
                f"{table_name}: rewrote {result.rewritten_manifests_count} manifests "
                f"into {result.added_manifests_count} manifests"
            )
        else:
            context.log.info(
                f"{table_name}: {total_manifests} manifests, no rewrite needed"
            )
```

### 8.5 Maintenance Schedule Summary

| Task | Schedule | Time (ET) | Duration | Impact |
|---|---|---|---|---|
| Small file compaction | Daily | 02:00 | 10--30 min | Improves query performance |
| Snapshot expiration | Daily | 03:00 | 1--5 min | Reclaims storage |
| Orphan file cleanup | Weekly (Sun) | 04:00 | 5--15 min | Reclaims storage |
| Manifest rewrite | Weekly (Sun) | 04:30 | 2--10 min | Improves scan planning |
| Statistics refresh | Daily | 03:15 | 5--10 min | Improves CBO |
| Cache warmup | Daily | 06:00 | 2--5 min | Improves first-query latency |

---

## 9. Benchmarking

### 9.1 Benchmark Queries

Define a fixed set of queries that represent real workloads. Run these consistently to detect regressions.

#### Point Lookup

```sql
-- Target: < 100 ms
-- Tests: Partition pruning, bucket pruning, index effectiveness
SELECT trade_id, symbol, price, quantity, timestamp, trade_type
FROM silver.fact_trades
WHERE trade_id = 'abc123-def456-ghi789';
```

#### Range Scan (Symbol + Date)

```sql
-- Target: < 1 second
-- Tests: Composite partition pruning, sort order effectiveness
SELECT timestamp, price, quantity, trade_type
FROM silver.fact_trades
WHERE symbol = 'AAPL'
  AND timestamp BETWEEN '2024-01-15 09:30:00' AND '2024-01-15 16:00:00'
ORDER BY timestamp;
```

#### Aggregation (Daily Volume)

```sql
-- Target: < 5 seconds
-- Tests: Full partition scan, aggregation performance, file count impact
SELECT
    symbol,
    DATE_TRUNC('day', timestamp) AS trade_date,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_volume,
    SUM(price * quantity) AS total_value,
    MIN(price) AS day_low,
    MAX(price) AS day_high
FROM silver.fact_trades
WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY symbol, DATE_TRUNC('day', timestamp)
ORDER BY total_value DESC;
```

#### Complex Join (Trader Performance)

```sql
-- Target: < 10 seconds
-- Tests: Join strategy, dimension broadcast, pre-aggregation
SELECT
    t.trader_id,
    d.trader_name,
    d.desk,
    s.sector,
    COUNT(*) AS trade_count,
    SUM(t.quantity * t.price) AS total_value,
    AVG(t.price) AS avg_price,
    SUM(CASE WHEN t.trade_type = 'BUY' THEN t.quantity ELSE -t.quantity END) AS net_position
FROM silver.fact_trades t
JOIN silver.dim_traders d ON t.trader_id = d.trader_id
JOIN silver.dim_symbols s ON t.symbol = s.symbol
WHERE t.timestamp BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY t.trader_id, d.trader_name, d.desk, s.sector
ORDER BY total_value DESC
LIMIT 100;
```

### 9.2 Benchmark Harness

```python
# tests/benchmarks/query_benchmark.py

import time
import statistics
import duckdb
from dataclasses import dataclass

@dataclass
class BenchmarkResult:
    query_name: str
    iterations: int
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    target_ms: float
    passed: bool

class QueryBenchmark:
    QUERIES = {
        "point_lookup": {
            "sql": """
                SELECT trade_id, symbol, price, quantity, timestamp, trade_type
                FROM iceberg_scan('silver.fact_trades')
                WHERE trade_id = ?
            """,
            "params": ["sample-trade-id"],
            "target_ms": 100,
            "iterations": 50,
        },
        "range_scan": {
            "sql": """
                SELECT timestamp, price, quantity, trade_type
                FROM iceberg_scan('silver.fact_trades')
                WHERE symbol = 'AAPL'
                  AND timestamp BETWEEN '2024-01-15 09:30:00' AND '2024-01-15 16:00:00'
                ORDER BY timestamp
            """,
            "params": [],
            "target_ms": 1000,
            "iterations": 20,
        },
        "daily_volume": {
            "sql": """
                SELECT symbol, DATE_TRUNC('day', timestamp) AS trade_date,
                       COUNT(*) AS cnt, SUM(quantity) AS vol
                FROM iceberg_scan('silver.fact_trades')
                WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
                GROUP BY symbol, DATE_TRUNC('day', timestamp)
                ORDER BY vol DESC
            """,
            "params": [],
            "target_ms": 5000,
            "iterations": 10,
        },
        "complex_join": {
            "sql": """
                SELECT t.trader_id, d.trader_name, s.sector,
                       COUNT(*) AS cnt, SUM(t.quantity * t.price) AS val
                FROM iceberg_scan('silver.fact_trades') t
                JOIN iceberg_scan('silver.dim_traders') d ON t.trader_id = d.trader_id
                JOIN iceberg_scan('silver.dim_symbols') s ON t.symbol = s.symbol
                WHERE t.timestamp BETWEEN '2024-01-01' AND '2024-03-31'
                GROUP BY t.trader_id, d.trader_name, s.sector
                ORDER BY val DESC LIMIT 100
            """,
            "params": [],
            "target_ms": 10000,
            "iterations": 5,
        },
    }

    def __init__(self):
        self.conn = duckdb.connect()

    def run_benchmark(self, query_name: str) -> BenchmarkResult:
        config = self.QUERIES[query_name]
        timings = []

        # Warmup run (not counted)
        self.conn.execute(config["sql"], config["params"]).fetchall()

        for _ in range(config["iterations"]):
            start = time.perf_counter()
            self.conn.execute(config["sql"], config["params"]).fetchall()
            elapsed_ms = (time.perf_counter() - start) * 1000
            timings.append(elapsed_ms)

        timings.sort()
        p95_idx = int(len(timings) * 0.95)
        p99_idx = int(len(timings) * 0.99)

        return BenchmarkResult(
            query_name=query_name,
            iterations=config["iterations"],
            min_ms=min(timings),
            max_ms=max(timings),
            mean_ms=statistics.mean(timings),
            median_ms=statistics.median(timings),
            p95_ms=timings[p95_idx],
            p99_ms=timings[p99_idx],
            target_ms=config["target_ms"],
            passed=statistics.median(timings) <= config["target_ms"],
        )

    def run_all(self) -> list[BenchmarkResult]:
        results = []
        for name in self.QUERIES:
            result = self.run_benchmark(name)
            results.append(result)
            status = "PASS" if result.passed else "FAIL"
            print(
                f"[{status}] {name}: "
                f"median={result.median_ms:.1f}ms "
                f"p95={result.p95_ms:.1f}ms "
                f"target={result.target_ms}ms"
            )
        return results
```

### 9.3 Load Testing

#### Ingestion Load Test

```python
# tests/load/ingestion_load_test.py

import asyncio
import json
import time
from kafka import KafkaProducer

class IngestionLoadTest:
    """
    Test sustained and burst ingestion rates.
    """

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
            batch_size=32768,
            compression_type="snappy",
        )

    def generate_trade(self, seq: int) -> dict:
        return {
            "trade_id": f"load-test-{seq}",
            "symbol": f"SYM{seq % 500:04d}",
            "price": 100.0 + (seq % 100) * 0.01,
            "quantity": 100 + (seq % 1000),
            "timestamp": int(time.time() * 1000),
            "trade_type": "BUY" if seq % 2 == 0 else "SELL",
            "exchange": "NYSE",
        }

    def sustained_test(self, rate: int = 1000, duration_sec: int = 300):
        """Sustained ingestion at {rate} trades/sec for {duration_sec} seconds."""
        interval = 1.0 / rate
        total = rate * duration_sec
        start = time.time()

        for i in range(total):
            trade = self.generate_trade(i)
            self.producer.send("raw.trades", value=trade, key=trade["symbol"].encode())

            # Throttle to target rate
            expected_time = start + (i + 1) * interval
            sleep_time = expected_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.producer.flush()
        elapsed = time.time() - start
        actual_rate = total / elapsed
        print(f"Sustained test: {total} trades in {elapsed:.1f}s = {actual_rate:.0f}/s (target: {rate}/s)")

    def burst_test(self, rate: int = 10000, duration_sec: int = 60):
        """Burst ingestion at {rate} trades/sec for {duration_sec} seconds."""
        total = rate * duration_sec
        start = time.time()

        for i in range(total):
            trade = self.generate_trade(i)
            self.producer.send("raw.trades", value=trade, key=trade["symbol"].encode())

        self.producer.flush()
        elapsed = time.time() - start
        actual_rate = total / elapsed
        print(f"Burst test: {total} trades in {elapsed:.1f}s = {actual_rate:.0f}/s (target: {rate}/s)")
```

#### API Concurrent Load Test (k6)

```javascript
// tests/load/api_load_test.js
// Run with: k6 run api_load_test.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const tradeQueryDuration = new Trend('trade_query_duration');

export const options = {
    scenarios: {
        // Sustained load: 50 concurrent users for 5 minutes
        sustained: {
            executor: 'constant-vus',
            vus: 50,
            duration: '5m',
        },
        // Spike test: Ramp to 200 users over 1 minute
        spike: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 200 },
                { duration: '1m', target: 200 },
                { duration: '30s', target: 0 },
            ],
            startTime: '6m',  // Start after sustained test
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<2000'],  // 95% of requests under 2s
        errors: ['rate<0.01'],               // Error rate under 1%
    },
};

const GRAPHQL_URL = 'http://localhost:8000/graphql';

const queries = [
    // Point lookup
    {
        query: `query { trade(id: "sample-id") { symbol price quantity } }`,
        name: 'point_lookup',
    },
    // Range query
    {
        query: `query { trades(symbol: "AAPL", date: "2024-01-15") { timestamp price quantity } }`,
        name: 'range_query',
    },
    // Aggregation
    {
        query: `query { dailyVolume(dateRange: { start: "2024-01-01", end: "2024-01-31" }) { symbol date volume } }`,
        name: 'aggregation',
    },
];

export default function () {
    const query = queries[Math.floor(Math.random() * queries.length)];

    const response = http.post(GRAPHQL_URL, JSON.stringify(query), {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: query.name },
    });

    check(response, {
        'status is 200': (r) => r.status === 200,
        'no errors': (r) => !JSON.parse(r.body).errors,
    });

    errorRate.add(response.status !== 200);
    tradeQueryDuration.add(response.timings.duration);

    sleep(Math.random() * 2);  // Random think time 0-2s
}
```

### 9.4 Performance Monitoring Over Time

```yaml
# prometheus/alerts/performance_alerts.yml

groups:
  - name: query_performance
    rules:
      - alert: SlowQueryP95
        expr: histogram_quantile(0.95, rate(query_duration_seconds_bucket[5m])) > 5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "P95 query latency exceeds 5 seconds"

      - alert: HighFileCount
        expr: iceberg_partition_file_count > 50
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Partition has more than 50 files; compaction needed"

      - alert: IngestionLag
        expr: kafka_consumer_lag_sum > 10000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Kafka consumer lag exceeds 10,000 messages"

      - alert: CacheHitRateLow
        expr: redis_cache_hit_rate < 0.5
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Cache hit rate below 50%; review cache strategy"
```

---

## 10. Implementation Steps

Concrete changes organized by module within the monorepo.

### Phase 1: Storage Layer Optimization (Week 1--2)

#### 10.1 Iceberg Table Configuration

```
Files to create/modify:
  infrastructure/iceberg/table-definitions/silver_fact_trades.sql
  infrastructure/iceberg/table-definitions/silver_fact_orderbook.sql
  infrastructure/iceberg/table-definitions/silver_dim_symbols.sql
  infrastructure/iceberg/table-definitions/gold_daily_summary.sql
  infrastructure/iceberg/catalog-config.properties
```

```sql
-- infrastructure/iceberg/table-definitions/silver_fact_trades.sql

CREATE TABLE silver.fact_trades (
    trade_id        STRING,
    symbol          STRING,
    price           DECIMAL(12, 4),
    quantity         BIGINT,
    timestamp       TIMESTAMP,
    trade_type      STRING,
    exchange        STRING,
    trader_id       STRING
)
USING iceberg
PARTITIONED BY (days(timestamp), bucket(8, symbol))
TBLPROPERTIES (
    'write.target-file-size-bytes' = '268435456',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.row-group-size-bytes' = '134217728',
    'write.parquet.dict-size-bytes' = '2097152',
    'write.parquet.bloom-filter-enabled.column.symbol' = 'true',
    'write.distribution-mode' = 'hash',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10',
    'commit.retry.num-retries' = '4',
    'read.split.target-size' = '134217728'
);

ALTER TABLE silver.fact_trades
  WRITE ORDERED BY symbol ASC, timestamp ASC;
```

#### 10.2 Kafka Topic Configuration

```
Files to create/modify:
  infrastructure/kafka/topics.sh
  infrastructure/kafka/producer.properties
  infrastructure/kafka/consumer.properties
```

```bash
# infrastructure/kafka/topics.sh

#!/bin/bash
BOOTSTRAP="localhost:9092"

declare -A TOPICS=(
    ["raw.trades"]=12
    ["raw.orderbook"]=12
    ["raw.marketdata"]=12
    ["processed.trades"]=12
    ["processed.orderbook"]=12
    ["alerts.anomalies"]=4
    ["dlq.failed-records"]=4
)

for TOPIC in "${!TOPICS[@]}"; do
    PARTITIONS=${TOPICS[$TOPIC]}
    kafka-topics.sh --bootstrap-server $BOOTSTRAP \
        --create --if-not-exists \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor 1 \
        --config retention.ms=604800000 \
        --config compression.type=snappy \
        --config segment.bytes=1073741824 \
        --config min.insync.replicas=1
    echo "Created topic: $TOPIC with $PARTITIONS partitions"
done
```

### Phase 2: Caching Layer (Week 2--3)

#### 10.3 Redis Cache Setup

```
Files to create/modify:
  infrastructure/docker/redis/redis.conf
  services/api/cache/__init__.py
  services/api/cache/redis_client.py
  services/api/cache/invalidation.py
  services/api/cache/decorators.py
```

```python
# services/api/cache/decorators.py

import functools
import hashlib
import json
from .redis_client import redis_client

def cached(prefix: str, ttl_seconds: int = 300):
    """
    Decorator to cache function results in Redis.

    Usage:
        @cached(prefix="trades", ttl_seconds=60)
        def get_trades(symbol: str, date: str):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Build cache key from function name + arguments
            key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
            key_hash = hashlib.sha256(key_data.encode()).hexdigest()[:16]
            cache_key = f"{prefix}:{func.__name__}:{key_hash}"

            # Try cache
            cached_result = redis_client.get(cache_key)
            if cached_result is not None:
                return json.loads(cached_result)

            # Cache miss: execute function
            result = func(*args, **kwargs)

            # Store in cache
            redis_client.setex(cache_key, ttl_seconds, json.dumps(result))

            return result
        return wrapper
    return decorator
```

### Phase 3: Query Optimization (Week 3--4)

#### 10.4 DuckDB Configuration

```
Files to create/modify:
  services/api/db/duckdb_config.py
  services/api/db/materialized_views.sql
  services/api/resolvers/optimized_trades.py
```

```python
# services/api/db/duckdb_config.py

import duckdb

def create_optimized_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with performance-optimized settings."""
    conn = duckdb.connect()

    conn.execute("SET threads = 4;")
    conn.execute("SET memory_limit = '4GB';")
    conn.execute("SET temp_directory = '/tmp/duckdb';")
    conn.execute("SET enable_object_cache = true;")
    conn.execute("SET enable_progress_bar = false;")
    conn.execute("SET preserve_insertion_order = false;")  # Allow reordering for speed

    # Install and load Iceberg extension
    conn.execute("INSTALL iceberg;")
    conn.execute("LOAD iceberg;")

    return conn
```

### Phase 4: Maintenance Jobs (Week 4--5)

#### 10.5 Dagster Maintenance Assets

```
Files to create/modify:
  dagster/assets/maintenance/__init__.py
  dagster/assets/maintenance/compaction.py
  dagster/assets/maintenance/snapshot_management.py
  dagster/assets/maintenance/orphan_cleanup.py
  dagster/assets/maintenance/manifest_rewrite.py
  dagster/assets/maintenance/statistics_refresh.py
  dagster/schedules/maintenance_schedules.py
```

```python
# dagster/schedules/maintenance_schedules.py

from dagster import ScheduleDefinition, DefaultScheduleStatus

daily_compaction = ScheduleDefinition(
    name="daily_compaction",
    cron_schedule="0 2 * * *",              # 2:00 AM ET daily
    target="compaction_job",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

daily_snapshot_expiration = ScheduleDefinition(
    name="daily_snapshot_expiration",
    cron_schedule="0 3 * * *",              # 3:00 AM ET daily
    target="snapshot_expiration_job",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

daily_statistics_refresh = ScheduleDefinition(
    name="daily_statistics_refresh",
    cron_schedule="15 3 * * *",             # 3:15 AM ET daily
    target="statistics_refresh_job",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

weekly_orphan_cleanup = ScheduleDefinition(
    name="weekly_orphan_cleanup",
    cron_schedule="0 4 * * 0",              # 4:00 AM ET every Sunday
    target="orphan_cleanup_job",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

weekly_manifest_rewrite = ScheduleDefinition(
    name="weekly_manifest_rewrite",
    cron_schedule="30 4 * * 0",             # 4:30 AM ET every Sunday
    target="manifest_rewrite_job",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)
```

### Phase 5: Benchmarking and Load Testing (Week 5--6)

#### 10.6 Benchmark Suite

```
Files to create/modify:
  tests/benchmarks/query_benchmark.py
  tests/benchmarks/conftest.py
  tests/benchmarks/test_query_performance.py
  tests/load/ingestion_load_test.py
  tests/load/api_load_test.js
  tests/load/Makefile
```

```makefile
# tests/load/Makefile

.PHONY: benchmark load-sustained load-burst load-api all

benchmark:
	python -m pytest tests/benchmarks/ -v --benchmark-json=benchmark-results.json

load-sustained:
	python tests/load/ingestion_load_test.py --mode sustained --rate 1000 --duration 300

load-burst:
	python tests/load/ingestion_load_test.py --mode burst --rate 10000 --duration 60

load-api:
	k6 run tests/load/api_load_test.js --out json=k6-results.json

all: benchmark load-sustained load-burst load-api
	@echo "All performance tests complete. Check *-results.json for details."
```

### Phase 6: Monitoring and Alerting (Week 6)

#### 10.7 Performance Dashboards

```
Files to create/modify:
  infrastructure/grafana/dashboards/performance.json
  infrastructure/prometheus/alerts/performance_alerts.yml
  services/api/middleware/metrics.py
```

```python
# services/api/middleware/metrics.py

from prometheus_client import Histogram, Counter, Gauge

# Query performance metrics
QUERY_DURATION = Histogram(
    "query_duration_seconds",
    "Time spent executing queries",
    ["query_type", "table"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

QUERY_ROWS_SCANNED = Counter(
    "query_rows_scanned_total",
    "Total rows scanned by queries",
    ["query_type", "table"],
)

CACHE_HITS = Counter(
    "cache_hits_total",
    "Cache hit count",
    ["cache_layer", "key_prefix"],
)

CACHE_MISSES = Counter(
    "cache_misses_total",
    "Cache miss count",
    ["cache_layer", "key_prefix"],
)

ICEBERG_FILES_SCANNED = Histogram(
    "iceberg_files_scanned",
    "Number of files scanned per query",
    ["table"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

PARTITION_FILE_COUNT = Gauge(
    "iceberg_partition_file_count",
    "Number of files per partition",
    ["table", "partition"],
)
```

---

## 11. Testing Strategy

### 11.1 Performance Regression Tests

Run as part of CI to catch performance regressions before they reach production.

```python
# tests/benchmarks/test_query_performance.py

import pytest
from .query_benchmark import QueryBenchmark

benchmark = QueryBenchmark()

class TestQueryPerformance:
    """
    Performance regression tests.
    These run in CI with a fixed dataset and assert that query latencies
    remain within acceptable bounds.

    The test dataset is loaded once during CI setup (see conftest.py).
    """

    def test_point_lookup_under_100ms(self):
        result = benchmark.run_benchmark("point_lookup")
        assert result.median_ms < 100, (
            f"Point lookup median {result.median_ms:.1f}ms exceeds 100ms target"
        )

    def test_range_scan_under_1s(self):
        result = benchmark.run_benchmark("range_scan")
        assert result.median_ms < 1000, (
            f"Range scan median {result.median_ms:.1f}ms exceeds 1000ms target"
        )

    def test_daily_volume_under_5s(self):
        result = benchmark.run_benchmark("daily_volume")
        assert result.median_ms < 5000, (
            f"Daily volume median {result.median_ms:.1f}ms exceeds 5000ms target"
        )

    def test_complex_join_under_10s(self):
        result = benchmark.run_benchmark("complex_join")
        assert result.median_ms < 10000, (
            f"Complex join median {result.median_ms:.1f}ms exceeds 10000ms target"
        )

    def test_no_full_table_scans(self):
        """Verify that partition pruning is effective for filtered queries."""
        result = benchmark.run_benchmark("range_scan")
        # In a well-partitioned table, range scan should read < 5% of files
        # This is checked via Iceberg scan metrics (see conftest.py setup)
        assert result.passed
```

### 11.2 Benchmark Suite Configuration

```python
# tests/benchmarks/conftest.py

import pytest
import duckdb

@pytest.fixture(scope="session")
def benchmark_db():
    """
    Create a DuckDB connection with a pre-loaded test dataset.
    The test dataset has:
    - 1,000,000 trades across 500 symbols over 30 days
    - 500 symbol dimension records
    - 100 trader dimension records
    """
    conn = duckdb.connect()
    conn.execute("SET threads = 4;")
    conn.execute("SET memory_limit = '4GB';")
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    # Load test data (generated by tests/fixtures/generate_test_data.py)
    conn.execute("CALL iceberg_scan_register('silver.fact_trades', 'test_data/silver/fact_trades');")
    conn.execute("CALL iceberg_scan_register('silver.dim_symbols', 'test_data/silver/dim_symbols');")
    conn.execute("CALL iceberg_scan_register('silver.dim_traders', 'test_data/silver/dim_traders');")

    yield conn
    conn.close()

@pytest.fixture(scope="session")
def benchmark_thresholds():
    """
    Performance thresholds. Adjust based on hardware.
    CI machines may need relaxed thresholds (multiply by 2x).
    """
    return {
        "point_lookup_ms": 100,
        "range_scan_ms": 1000,
        "daily_volume_ms": 5000,
        "complex_join_ms": 10000,
    }
```

### 11.3 Load Testing Targets

| Test | Tool | Target | Duration | Pass Criteria |
|---|---|---|---|---|
| Sustained ingestion | Python script | 1,000 trades/sec | 5 minutes | Zero dropped messages; consumer lag < 1,000 |
| Burst ingestion | Python script | 10,000 trades/sec | 1 minute | Consumer recovers within 2 minutes after burst |
| API concurrent load | k6 | 50 VUs | 5 minutes | P95 < 2s; error rate < 1% |
| API spike test | k6 | 200 VUs peak | 2 minutes | P95 < 5s; error rate < 5%; no crashes |
| End-to-end pipeline | Custom harness | 1,000 trades/sec | 30 minutes | Data arrives in Iceberg within 30s; dashboards update within 1 min |

### 11.4 Continuous Performance Monitoring

```yaml
# .github/workflows/performance.yml (or local CI equivalent)

name: Performance Tests
on:
  push:
    branches: [main]
  schedule:
    - cron: '0 6 * * 1'  # Weekly Monday 6 AM

jobs:
  benchmark:
    runs-on: self-hosted  # Consistent hardware for reproducible results
    steps:
      - uses: actions/checkout@v4

      - name: Setup test environment
        run: |
          make setup-test-env
          python tests/fixtures/generate_test_data.py

      - name: Run benchmark suite
        run: |
          make benchmark
          python tests/benchmarks/compare_results.py \
            --baseline benchmark-baseline.json \
            --current benchmark-results.json \
            --threshold 1.2  # Fail if >20% regression

      - name: Run load tests
        run: |
          make load-sustained
          make load-api

      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-results
          path: |
            benchmark-results.json
            k6-results.json
```

### 11.5 Test Data Generation

```python
# tests/fixtures/generate_test_data.py

"""
Generate a reproducible test dataset for benchmarking.
Uses a fixed seed for deterministic results across runs.
"""

import random
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta

SEED = 42
NUM_TRADES = 1_000_000
NUM_SYMBOLS = 500
NUM_TRADERS = 100
DATE_RANGE_DAYS = 30
START_DATE = datetime(2024, 1, 1, 9, 30, 0)

random.seed(SEED)

SYMBOLS = [f"SYM{i:04d}" for i in range(NUM_SYMBOLS)]
TRADERS = [f"TRADER{i:04d}" for i in range(NUM_TRADERS)]
EXCHANGES = ["NYSE", "NASDAQ", "CBOE", "ARCA", "BATS"]

def generate_trades():
    trades = []
    for i in range(NUM_TRADES):
        trade_date = START_DATE + timedelta(
            days=random.randint(0, DATE_RANGE_DAYS - 1),
            hours=random.randint(0, 6),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        trades.append({
            "trade_id": f"TEST-{i:010d}",
            "symbol": random.choice(SYMBOLS),
            "price": round(random.uniform(1.0, 5000.0), 4),
            "quantity": random.randint(1, 10000),
            "timestamp": trade_date,
            "trade_type": random.choice(["BUY", "SELL"]),
            "exchange": random.choice(EXCHANGES),
            "trader_id": random.choice(TRADERS),
        })
    return trades

if __name__ == "__main__":
    print(f"Generating {NUM_TRADES:,} test trades...")
    trades = generate_trades()

    table = pa.Table.from_pylist(trades)
    pq.write_table(table, "test_data/silver/fact_trades/data.parquet",
                    row_group_size=128 * 1024 * 1024)

    print(f"Generated {len(trades):,} trades to test_data/silver/fact_trades/")
```

---

## Summary

| Area | Key Decision | Expected Impact |
|---|---|---|
| Partitioning | `days(timestamp)` + `bucket(8, symbol)` | 99%+ partition pruning on typical queries |
| Sort order | `(symbol, timestamp)` within partitions | Row group skipping for symbol filters |
| File sizing | 256 MB target | Balance parallelism vs overhead |
| Caching | Redis (app) + DuckDB (query) + OS (data) | Sub-second responses for repeated queries |
| Predicate pushdown | Partition + row group + page level | 10--100x I/O reduction |
| Compaction | Daily at 2 AM, trigger at 50 files | Prevent small-file degradation |
| Snapshots | Expire > 7 days, retain >= 3 | Storage reclamation with time-travel safety |
| Benchmarks | 4 query types with latency targets | Catch regressions before production |
| Load testing | 1K sustained / 10K burst / 50 VU API | Validate end-to-end throughput |
