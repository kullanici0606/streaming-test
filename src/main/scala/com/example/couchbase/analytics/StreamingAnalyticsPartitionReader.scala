/*
 * A streaming replacement for com.couchbase.spark.analytics.AnalyticsPartitionReader.
 *
 * The stock reader calls the BLOCKING Scala SDK (cluster.analyticsQuery) and then does
 *
 *   result.flatMap(_.rowsAs[String](...)).get.flatMap(parser.parse(...))
 *
 * which materialises the whole result set three times over (SDK row buffer, Seq[String],
 * strict Seq[InternalRow]) before Spark ever pulls a row. For large datasets that is an OOM.
 *
 * This version subscribes to the REACTIVE result and drives demand from Spark's own
 * next()/get() calls, so at most DesiredItemsInQueue rows are ever in flight -- the same
 * fix Couchbase applied to QueryPartitionReader under SPARKC-178.
 *
 * Licensed under the Apache License, Version 2.0; derived from the Couchbase Spark
 * Connector (Copyright (c) 2021 Couchbase, Inc.).
 */

package com.example.couchbase.analytics

import com.couchbase.client.scala.analytics.{
  AnalyticsMetaData,
  AnalyticsScanConsistency,
  ReactiveAnalyticsResult,
  AnalyticsOptions => CouchbaseAnalyticsOptions
}
import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.spark.analytics.{AnalyticsOptions, AnalyticsPartitionReader, AnalyticsReadConfig}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.CouchbaseJsonUtils
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.reactivestreams.{Subscriber, Subscription}
import reactor.core.scala.publisher.{SFlux, SMono}
import reactor.core.scheduler.Schedulers

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.duration.{Duration, NANOSECONDS}

class StreamingAnalyticsPartitionReader(
    schema: StructType,
    conf: CouchbaseConfig,
    readConfig: AnalyticsReadConfig,
    filters: Array[Filter],
    aggregations: Option[Aggregation]
) extends PartitionReader[InternalRow]
    with Logging {

  /** Upper bound on rows held in memory: outstanding reactive demand plus whatever is sitting
    * in the hand-off queue. This is the whole point of the class -- keep it small.
    */
  private val DesiredItemsInQueue = 30

  private val parser       = CouchbaseJsonUtils.jsonParser(schema)
  private val createParser = CouchbaseJsonUtils.createParser()

  private val queue      = new ConcurrentLinkedQueue[InternalRow]
  private val error      = new AtomicReference[Throwable]()
  private val isComplete = new AtomicBoolean()
  private val subscription                = new AtomicReference[Subscription]()
  private val requestedButNotYetConsumed  = new AtomicInteger()
  private val metaData                    = new AtomicReference[AnalyticsMetaData]()
  private val reactiveResult              = new AtomicReference[ReactiveAnalyticsResult]()
  private val started                     = System.nanoTime

  private val groupByColumns = aggregations match {
    case Some(agg) => agg.groupByExpressions().map(n => n.references().head.fieldNames().head).toSeq
    case None      => Seq.empty
  }

  private val query = buildAnalyticsQuery()

  private val result: SMono[ReactiveAnalyticsResult] = {
    if (readConfig.bucket.isEmpty || readConfig.scope.isEmpty) {
      logInfo(s"Running streaming analytics query $query")
      CouchbaseConnection(readConfig.connectionIdentifier)
        .cluster(conf)
        .reactive
        .analyticsQuery(query, buildOptions())
        .subscribeOn(Schedulers.boundedElastic())
    } else {
      logInfo(
        s"Running streaming analytics query $query against ${readConfig.bucket.get}.${readConfig.scope.get}"
      )
      CouchbaseConnection(readConfig.connectionIdentifier)
        .cluster(conf)
        .bucket(readConfig.bucket.get)
        .scope(readConfig.scope.get)
        .reactive
        .analyticsQuery(query, buildOptions())
        .subscribeOn(Schedulers.boundedElastic())
    }
  }

  // Subscribe straight to the row flux (rather than to a `.then()`-flattened stream, which would
  // request unbounded and defeat the point) so that our request(n) reaches the SDK's chunk parser.
  private val rows: SFlux[String] = result.flatMapMany { r =>
    reactiveResult.set(r)
    r.rowsAs[String](Passthrough.StringConvert)
  }

  rows.subscribe(new Subscriber[String] {
    override def onSubscribe(s: Subscription): Unit = subscription.set(s)

    override def onNext(row: String): Unit = processRow(row)

    override def onError(err: Throwable): Unit = {
      logError(
        s"Error on analytics query $query after ${NANOSECONDS.toMillis(System.nanoTime() - started)}ms",
        err
      )
      error.set(err)
    }

    override def onComplete(): Unit = {
      Option(reactiveResult.get())
        .foreach(r => r.meta.doOnNext(md => metaData.set(md)).subscribe())
      logInfo(
        s"Completed analytics query $query in ${NANOSECONDS.toMillis(System.nanoTime() - started)}ms"
      )
      isComplete.set(true)
    }
  })

  private def processRow(r: String): Unit = {
    var row = r
    try {
      // Aggregates like MIN, MAX etc come back as $1, $2 ... so they need to be replaced with
      // their original field names from the schema for the JSON parser to pick them up.
      if (hasAggregateFields) {
        var idx = 1
        schema.fields.foreach(field => {
          if (!groupByColumns.contains(field.name)) {
            row = row.replace("$" + idx, field.name)
            idx = idx + 1
          }
        })
      }
      val parsed = parser.parse(row, createParser, UTF8String.fromString).toSeq
      if (parsed.size != 1) {
        throw new IllegalStateException(s"Expected 1 row, have $parsed")
      }
      queue.add(parsed.head)
    } catch {
      case e: Exception =>
        error.set(
          new IllegalStateException(s"Could not parse row $row based on provided schema $schema.", e)
        )
    }
  }

  override def next(): Boolean = {
    var isDone  = false
    var hasItem = false

    while (!isDone) {
      // Top the outstanding demand back up. Guarded because with subscribeOn the subscription is
      // normally set synchronously, but we must never call request() before onSubscribe.
      val sub = subscription.get()
      if (sub != null) {
        val needToAdd = DesiredItemsInQueue - requestedButNotYetConsumed.get
        if (needToAdd > 0) {
          requestedButNotYetConsumed.addAndGet(needToAdd)
          sub.request(needToAdd)
        }
      }

      if (error.get != null) {
        throw error.get()
      }

      if (queue.peek() != null) {
        isDone = true
        hasItem = true
      } else if (isComplete.get) {
        isDone = true
      } else {
        Thread.sleep(1)
      }
    }

    hasItem
  }

  override def get(): InternalRow = {
    val out = queue.poll()
    requestedButNotYetConsumed.decrementAndGet()
    out
  }

  // Unlike the stock reader, cancel the in-flight query when Spark stops early (LIMIT, task kill,
  // downstream failure) instead of leaving the analytics request running.
  override def close(): Unit = {
    val sub = subscription.get()
    if (sub != null && !isComplete.get) {
      sub.cancel()
    }
    queue.clear()
  }

  def buildAnalyticsQuery(): String = {
    var fields = schema.fields
      .map(f => f.name)
      .filter(f => !f.equals(readConfig.idFieldName))
      .map(f => AnalyticsPartitionReader.maybeEscapeField(f))
    if (!hasAggregateFields) {
      fields = fields :+ s"META().id as `${readConfig.idFieldName}`"
    }

    var predicate       = readConfig.userFilter.map(p => s" WHERE $p").getOrElse("")
    val compiledFilters = AnalyticsPartitionReader.compileFilter(filters)
    if (compiledFilters.nonEmpty && predicate.nonEmpty) {
      predicate = predicate + " AND " + compiledFilters
    } else if (compiledFilters.nonEmpty) {
      predicate = " WHERE " + compiledFilters
    }

    val groupBy = if (hasAggregateGroupBy) {
      " GROUP BY " + aggregations.get
        .groupByExpressions()
        .map(n => s"`${n.references().head}`")
        .mkString(", ")
    } else {
      ""
    }

    val fieldsEncoded = fields.mkString(", ")
    s"select $fieldsEncoded from `${readConfig.dataset}`$predicate$groupBy"
  }

  def buildOptions(): CouchbaseAnalyticsOptions = {
    var opts = CouchbaseAnalyticsOptions()
    readConfig.scanConsistency match {
      case AnalyticsOptions.NotBoundedScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.NotBounded)
      case AnalyticsOptions.RequestPlusScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.RequestPlus)
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }
    readConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def hasAggregateFields: Boolean = aggregations match {
    case Some(a) => !a.aggregateExpressions().isEmpty
    case None    => false
  }

  def hasAggregateGroupBy: Boolean = aggregations match {
    case Some(a) => !a.groupByExpressions().isEmpty
    case None    => false
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    AnalyticsPartitionReader.currentMetricsValues(Option(metaData.get()).map(_.metrics))
}
