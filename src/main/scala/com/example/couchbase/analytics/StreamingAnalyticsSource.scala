/*
 * DataSource v2 plumbing that routes `couchbase.analytics.streaming` to
 * StreamingAnalyticsPartitionReader. Everything here is a thin subclass of the stock
 * connector classes except StreamingAnalyticsScanBuilder, which has to be copied because
 * AnalyticsScanBuilder keeps its pushdown state in private fields.
 *
 * Licensed under the Apache License, Version 2.0; derived from the Couchbase Spark
 * Connector (Copyright (c) 2021 Couchbase, Inc.).
 */

package com.example.couchbase.analytics

import com.couchbase.spark.analytics.{
  AnalyticsBatch,
  AnalyticsInputPartition,
  AnalyticsReadConfig,
  AnalyticsScan,
  AnalyticsTable,
  AnalyticsTableProvider
}
import com.couchbase.spark.config.CouchbaseConfig
import com.couchbase.spark.query.QueryAggregations
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class StreamingAnalyticsTableProvider extends AnalyticsTableProvider {

  override def shortName(): String = "couchbase.analytics.streaming"

  // inferSchema() is inherited unchanged: it runs with LIMIT <inferLimit> (1000 by default),
  // so its buffering is bounded and harmless.
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table =
    new StreamingAnalyticsTable(schema, partitioning, properties, readConfig(properties))
}

class StreamingAnalyticsTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    readConfig: AnalyticsReadConfig
) extends AnalyticsTable(schema, partitioning, properties, readConfig) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StreamingAnalyticsScanBuilder(schema, readConfig)
}

class StreamingAnalyticsScanBuilder(schema: StructType, readConfig: AnalyticsReadConfig)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates {

  private var finalSchema                       = schema
  private var pushedFilter                      = Array.empty[Filter]
  private var aggregations: Option[Aggregation] = None

  override def build(): Scan =
    new StreamingAnalyticsScan(finalSchema, readConfig, pushedFilter, aggregations)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilter = filters
    Array.empty[Filter]
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema != null && requiredSchema.nonEmpty) {
      finalSchema = requiredSchema
    }
  }

  override def pushAggregation(agg: Aggregation): Boolean = {
    if (!readConfig.pushDownAggregate) {
      return false
    }

    val aggregateFuncs = QueryAggregations.convertAggregateExpressions(agg, schema)
    if (aggregateFuncs.isEmpty) {
      return false
    }

    finalSchema = if (agg.groupByExpressions().isEmpty) {
      StructType(aggregateFuncs)
    } else {
      val grouped = QueryAggregations.convertGroupByExpression(agg, schema)
      if (grouped.isEmpty) {
        return false
      } else {
        StructType(grouped ++ aggregateFuncs)
      }
    }

    aggregations = Some(agg)
    true
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean =
    QueryAggregations.supportsCompleteAggPushdown(aggregation)
}

class StreamingAnalyticsScan(
    schema: StructType,
    readConfig: AnalyticsReadConfig,
    filters: Array[Filter],
    aggregations: Option[Aggregation]
) extends AnalyticsScan(schema, readConfig, filters, aggregations) {

  private lazy val conf =
    CouchbaseConfig(SparkSession.active.sparkContext.getConf, readConfig.connectionIdentifier)

  override def toBatch: Batch =
    new StreamingAnalyticsBatch(schema, conf, readConfig, filters, aggregations)
}

class StreamingAnalyticsBatch(
    schema: StructType,
    conf: CouchbaseConfig,
    readConfig: AnalyticsReadConfig,
    filters: Array[Filter],
    aggregations: Option[Aggregation]
) extends AnalyticsBatch(schema, conf, readConfig, filters, aggregations) {

  // planInputPartitions() is inherited: a single partition with analytics-node preferred locations.
  override def createReaderFactory(): PartitionReaderFactory =
    new StreamingAnalyticsPartitionReaderFactory(conf, readConfig)
}

class StreamingAnalyticsPartitionReaderFactory(
    conf: CouchbaseConfig,
    readConfig: AnalyticsReadConfig
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val part = partition.asInstanceOf[AnalyticsInputPartition]
    new StreamingAnalyticsPartitionReader(
      part.schema,
      conf,
      readConfig,
      part.filters,
      part.aggregations
    )
  }
}
