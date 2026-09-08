/*
 * Read options specific to the streaming analytics source. The stock AnalyticsReadConfig is a
 * closed case class, so anything extra travels next to it in this one.
 *
 * Licensed under the Apache License, Version 2.0.
 */

package com.example.couchbase.analytics

import java.util

object StreamingAnalyticsOptions {

  /** Upper bound on rows held per task: outstanding reactive demand plus rows parsed and waiting
    * in the hand-off queue for Spark. See StreamingAnalyticsPartitionReader.
    */
  val QueueSize        = "queueSize"
  val DefaultQueueSize = 30
}

case class StreamingAnalyticsReadConfig(queueSize: Int)

object StreamingAnalyticsReadConfig {

  def apply(properties: util.Map[String, String]): StreamingAnalyticsReadConfig = {
    val queueSize = Option(properties.get(StreamingAnalyticsOptions.QueueSize)) match {
      case None => StreamingAnalyticsOptions.DefaultQueueSize
      case Some(raw) =>
        val parsed =
          try raw.trim.toInt
          catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(
                s"Option '${StreamingAnalyticsOptions.QueueSize}' must be a positive integer, got '$raw'"
              )
          }
        if (parsed < 1) {
          throw new IllegalArgumentException(
            s"Option '${StreamingAnalyticsOptions.QueueSize}' must be a positive integer, got '$raw'"
          )
        }
        parsed
    }
    StreamingAnalyticsReadConfig(queueSize)
  }
}
