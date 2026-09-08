package com.example.couchbase.analytics

import org.scalatest.funsuite.AnyFunSuite

import java.util

class StreamingAnalyticsReadConfigSpec extends AnyFunSuite {

  private def props(kv: (String, String)*): util.Map[String, String] = {
    val m = new util.HashMap[String, String]()
    kv.foreach { case (k, v) => m.put(k, v) }
    m
  }

  test("queueSize defaults to 30 when absent") {
    assert(StreamingAnalyticsReadConfig(props()).queueSize == 30)
    assert(StreamingAnalyticsOptions.DefaultQueueSize == 30)
  }

  test("queueSize is read from the options") {
    assert(StreamingAnalyticsReadConfig(props("queueSize" -> "512")).queueSize == 512)
    assert(StreamingAnalyticsReadConfig(props("queueSize" -> " 8 ")).queueSize == 8)
  }

  test("queueSize rejects non-positive and non-numeric values") {
    Seq("0", "-5", "abc", "1.5", "").foreach { bad =>
      val e = intercept[IllegalArgumentException] {
        StreamingAnalyticsReadConfig(props("queueSize" -> bad))
      }
      assert(e.getMessage.contains("queueSize"), s"message for '$bad' was: ${e.getMessage}")
    }
  }
}
