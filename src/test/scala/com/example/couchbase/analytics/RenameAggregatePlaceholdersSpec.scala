package com.example.couchbase.analytics

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class RenameAggregatePlaceholdersSpec extends AnyFunSuite {

  import StreamingAnalyticsPartitionReader.renameAggregatePlaceholders

  private def aggSchema(n: Int): StructType =
    StructType((1 to n).map(i => StructField(s"MIN(`c$i`)", IntegerType)))

  private def aggRow(n: Int): String =
    (1 to n).map(i => s""""$$$i":${i * 100}""").mkString("{", ",", "}")

  test("fewer than ten aggregates are renamed in schema order") {
    val out = renameAggregatePlaceholders(aggRow(3), aggSchema(3), Seq.empty)
    assert(out == """{"MIN(`c1`)":100,"MIN(`c2`)":200,"MIN(`c3`)":300}""")
  }

  test("ten or more aggregates keep their own names") {
    val out = renameAggregatePlaceholders(aggRow(12), aggSchema(12), Seq.empty)
    (1 to 12).foreach { i =>
      assert(out.contains(s""""MIN(`c$i`)":${i * 100}"""), s"aggregate $i mangled in $out")
    }
    assert(!out.contains("MIN(`c1`)0"), s"prefix of $$10 was rewritten: $out")
    assert(!out.contains("$"), s"unreplaced placeholder left in $out")
  }

  test("group-by columns are skipped when numbering placeholders") {
    val schema = StructType(
      Seq(
        StructField("region", StringType),
        StructField("SUM(`amount`)", LongType),
        StructField("year", IntegerType),
        StructField("COUNT(*)", LongType)
      )
    )
    val row = """{"region":"eu","$1":42,"year":2024,"$2":7}"""
    val out = renameAggregatePlaceholders(row, schema, Seq("region", "year"))
    assert(out == """{"region":"eu","SUM(`amount`)":42,"year":2024,"COUNT(*)":7}""")
  }

  test("rows without placeholders are returned unchanged") {
    val row = """{"region":"eu","year":2024}"""
    assert(renameAggregatePlaceholders(row, aggSchema(0), Seq.empty) == row)
  }
}
