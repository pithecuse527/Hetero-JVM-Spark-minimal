package com.research.gcaware

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, lit, pmod, sum}

/**
 * TPC-H post-aggregate allocation workload.
 *
 * Usage: TpchArrayAmplifierRunner <tpch-parquet-root>
 *
 * All experiment knobs are Spark confs so the identical jar can be submitted
 * to either JVM pool.  The UDF is intentionally applied only after the
 * orders/lineitem join and HashAggregate have produced one row per order key.
 */
object TpchArrayAmplifierRunner {
  private val Version = "tpch-array-amplifier-v1"

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: TpchArrayAmplifierRunner <tpch-parquet-root>")
      System.exit(2)
    }

    val spark = SparkSession.builder().getOrCreate()
    try {
      val conf = spark.sparkContext.getConf
      val root = args(0).stripSuffix("/")
      val format = conf.get("spark.gcaware.tpch.format", "parquet")
      val elements = conf.getInt("spark.gcaware.array.elements", 100000)
      val modulus = conf.getInt("spark.gcaware.tpch.groupModulus", 300000)
      val buckets = conf.getInt("spark.gcaware.tpch.groupBuckets", 1000)
      val shipDateLower = conf.get("spark.gcaware.tpch.shipDateLower", "")
      val shipDateUpper = conf.get("spark.gcaware.tpch.shipDateUpper", "")

      require(elements > 0 && elements <= 5000000,
        s"spark.gcaware.array.elements must be in 1..5000000, got $elements")
      require(modulus > 0, s"spark.gcaware.tpch.groupModulus must be positive, got $modulus")
      require(buckets > 0 && buckets <= modulus,
        s"spark.gcaware.tpch.groupBuckets must be in 1..$modulus, got $buckets")

      // A Spark native JVM UDF: portable bytecode on HotSpot and OpenJ9. Spark
      // generates the call site but boxes its SQL inputs at the UDF boundary.
      spark.udf.register("array_amplify",
        (orderKey: Long, revenue: Double, n: Int) => ArrayAmplifierUdf.score(orderKey, revenue, n))

      val orders = spark.read.format(format).load(s"$root/orders")
        .select(col("o_orderkey").cast("long").as("order_key"))
        // This controls group cardinality before the join; it is deterministic
        // and avoids a LIMIT that could alter or short-circuit the physical plan.
        .where(pmod(col("order_key"), lit(modulus)) < lit(buckets))

      var lineitem = spark.read.format(format).load(s"$root/lineitem")
        .select(
          col("l_orderkey").cast("long").as("order_key"),
          col("l_extendedprice").cast("double").as("extended_price"),
          col("l_discount").cast("double").as("discount"),
          col("l_shipdate").as("ship_date"))
      if (shipDateLower.nonEmpty) lineitem = lineitem.where(col("ship_date") >= lit(shipDateLower))
      if (shipDateUpper.nonEmpty) lineitem = lineitem.where(col("ship_date") < lit(shipDateUpper))

      val grouped = orders.join(lineitem, Seq("order_key"))
        .groupBy(col("order_key"))
        .agg(sum(col("extended_price") * (lit(1.0) - col("discount"))).as("revenue"))

      // The outer aggregation consumes the scalar result, preventing Catalyst
      // from dropping the UDF projection while keeping the large array transient.
      val scored = grouped.selectExpr(
        s"array_amplify(order_key, revenue, $elements) AS amplified_score")
      println(s"ARRAY_UDF_INFO:version=$Version elements=$elements bytes=${elements.toLong * 8L} " +
        s"dataRoot=$root format=$format groupModulus=$modulus groupBuckets=$buckets " +
        s"shipDateLower=${if (shipDateLower.nonEmpty) shipDateLower else "<none>"} " +
        s"shipDateUpper=${if (shipDateUpper.nonEmpty) shipDateUpper else "<none>"}")
      println("--- ARRAY_UDF_EXPLAIN ---")
      scored.explain(true)

      val started = System.nanoTime()
      val result = scored.agg(
        count(lit(1)).as("group_count"),
        sum(col("amplified_score")).as("checksum")).collect().head
      val elapsedMs = (System.nanoTime() - started) / 1e6
      println(s"ARRAY_UDF_RESULT:groups=${result.getLong(0)} checksum=${result.getDouble(1)} " +
        s"durationMs=${"%.2f".format(elapsedMs)} appId=${spark.sparkContext.applicationId}")
    } finally {
      spark.stop()
    }
  }
}

/** One deterministic on-heap double[] allocation per UDF invocation. */
object ArrayAmplifierUdf {
  private val Multiplier = 2862933555777941757L
  private val Increment = 3037000493L
  private val Unit53 = 1.1102230246251565e-16 // 2^-53

  def score(orderKey: Long, revenue: Double, elements: Int): Double = {
    // This is the only deliberately large allocation in the UDF.
    val values = new Array[Double](elements)
    var state = orderKey ^ java.lang.Double.doubleToRawLongBits(revenue)
    var i = 0
    while (i < elements) {
      state = state * Multiplier + Increment
      values(i) = ((state >>> 11).toDouble * Unit53) + revenue * 1.0e-9
      i += 1
    }
    var total = 0.0
    i = 0
    while (i < elements) {
      total += values(i)
      i += 1
    }
    total / elements
  }
}
