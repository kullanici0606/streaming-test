# Streaming Couchbase Analytics reader for Spark

Replaces the Couchbase Spark Connector's `couchbase.analytics` reader, which buffers the
entire result set in memory, with a backpressured streaming one registered as
**`couchbase.analytics.streaming`**.

## Why

`com.couchbase.spark.analytics.AnalyticsPartitionReader` (connector 3.5.x and 4.0.0) does:

```scala
private lazy val result = cluster.analyticsQuery(...)          // blocking SDK: buffers all rows
private lazy val rows = result.flatMap(_.rowsAs[String](...))
  .get                                                          // Seq[String]      - all rows again
  .flatMap(r => parser.parse(r, ...))                            // strict Seq[InternalRow] - and again
private lazy val rowIterator = rows.iterator                     // iterator over a finished Seq
```

`result` and `rows` are `lazy val` *fields*, so all three copies stay reachable for the life of
the reader. At ~2 KB/doc and 10M docs that is well over 20 GB inside a single task, allocated
before Spark sees row one — no amount of repartitioning or executor tuning helps.

`QueryPartitionReader` does not have this problem: Couchbase fixed it under **SPARKC-178** using
the reactive SDK plus a bounded hand-off queue. The Analytics reader never got that treatment.
This module applies the same fix, subscribing directly to the row flux so `request(n)` reaches
the SDK's chunk parser. Steady state is `queueSize` (default 30) parsed rows plus the SDK's own
256-row prefetch buffer in memory.

## Usage

```java
Dataset<Row> df = spark.read()
    .format("couchbase.analytics.streaming")   // was: "couchbase.analytics"
    .schema(schema)                            // optional but recommended
    .option("dataset", "my_dataset")
    .option("bucket", "my-bucket")
    .option("scope", "my-scope")
    .option("filter", "type = 'foo'")
    .load();
```

All options, filter/column/aggregate pushdown and schema inference behave exactly as the stock
`couchbase.analytics` source — the plumbing subclasses the connector's own classes.

One extra option:

| Option | Default | Meaning |
| --- | --- | --- |
| `queueSize` | `30` | Upper bound on rows held per task: outstanding demand to the SDK plus rows already parsed and waiting for Spark. Must be a positive integer. |

```java
spark.read()
    .format("couchbase.analytics.streaming")
    .option("dataset", "my_dataset")
    .option("queueSize", "1000")
    .load();
```

### Choosing `queueSize`

`queueSize` bounds memory, but because `next()` polls the hand-off queue with a 1 ms sleep it
also caps throughput. When Spark drains the queue faster than the SDK thread fills it, each
sleep cycle moves at most `queueSize` rows, so the ceiling is roughly `queueSize` × 1000 rows/s
per task:

| `queueSize` | Ceiling (rows/s) | 10M rows, best case |
| --- | --- | --- |
| 30 (default) | ~30k | ~5.5 min |
| 256 | ~256k | ~40 s |
| 1000 | ~1M | ~10 s |

The ceiling only matters if the SDK-side JSON parsing is faster than it, which for documents of
a few KB it usually is. Memory cost is `queueSize` parsed rows per task, roughly the JSON size
times two to three, so 1000 rows of 2 KB documents is a few MB. Set it to a few hundred or
1000 for large reads; there is nothing to gain above that.

Two things `queueSize` does **not** control:

- **Socket reads.** The SDK keeps its own 256-row prefetch buffer between the network and this
  reader and re-requests from the socket as that buffer drains, so the network is paused and
  resumed at ~256-row granularity whatever `queueSize` is.
- **Parallelism.** The analytics batch plans a single input partition, so one task, one
  connection and one parser thread handle the whole result. For very large datasets, split
  the read into several `load()` calls with disjoint `filter` options and union them.

The default is the value the stock `QueryPartitionReader` uses. It is kept for compatibility
and is too low for multi-million-row reads.

## Build & deploy

```bash
mvn package
spark-submit --jars couchbase-analytics-streaming-1.0.0.jar,spark-connector_2.12-3.5.5.jar ...
```

Set `scala.binary.version`, `spark.version` and `couchbase.connector.version` in `pom.xml` to
match your deployment.

## Files

| File | Role |
| --- | --- |
| `StreamingAnalyticsPartitionReader.scala` | the reader — reactive subscribe + bounded queue |
| `StreamingAnalyticsSource.scala` | DSv2 wiring: TableProvider → Table → ScanBuilder → Scan → Batch → ReaderFactory |
| `StreamingAnalyticsReadConfig.scala` | parses the module's own read options (`queueSize`) |
| `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` | registers the format name |

Everything in the wiring is a thin subclass of the connector's classes except
`StreamingAnalyticsScanBuilder`, which had to be copied because `AnalyticsScanBuilder` keeps its
pushdown state (`finalSchema`, `pushedFilter`, `aggregations`) in private fields.

## Differences from the stock reader, beyond streaming

- `close()` cancels the in-flight query. The stock reader's `close()` is empty, so an early stop
  (LIMIT, task kill, downstream failure) leaves the analytics request running server-side.
- Metrics (`currentMetricsValues`) are populated from the reactive `meta` mono on completion, so
  they appear once the partition finishes rather than up front.
- Aggregate placeholder renaming (`$1`, `$2`, ...) runs from the highest index down. The stock
  reader replaces `$1` first, which also rewrites the prefix of `$10`, `$11`, ... and returns null
  for every pushed-down aggregate beyond the ninth. Covered by `RenameAggregatePlaceholdersSpec`
  (`mvn test`).

## Verification status

- Compiles clean against Scala 2.12.18 / Spark 3.5.9 / connector 3.5.5.
- `couchbase.analytics.streaming` confirmed discoverable via Spark's `DataSourceRegister`
  ServiceLoader, with the full wiring chain loading.
- **Not** run against a live Couchbase Analytics cluster — no cluster available here. Validate
  memory behaviour on a real dataset before you rely on it.
