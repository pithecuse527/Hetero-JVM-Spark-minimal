package com.research.gcaware

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

/**
 * TPC-H join/group workload whose only JVM-specific pressure is a transient
 * primitive-array allocation inside a scalar UDF.
 *
 * Arguments intentionally match run-screening.sh:
 *   <query> <scale> <dataLocation>
 *
 * The query and UDF knobs are Spark confs so the same jar and SQL are used on
 * HotSpot and OpenJ9:
 *   spark.gcaware.arrayN       number of doubles allocated per UDF call
 *   spark.gcaware.groupCount   hash buckets/groups after the join
 *   spark.gcaware.dateStart    inclusive TPC-H l_shipdate
 *   spark.gcaware.dateEnd      exclusive TPC-H l_shipdate
 */
object HeapArrayAmplifierRunner {
  private val QueryName = "heap-amplifier"

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: <query> <scale> <dataLocation>")
      System.exit(2)
    }
    require(args(0) == QueryName, s"heapamplifier query must be $QueryName")

    val scale = args(1)
    val dataLocation = args(2).stripSuffix("/")
    val suppliedConf = new SparkConf()
    val arrayN = suppliedConf.getInt("spark.gcaware.arrayN", 1024)
    val groupCount = suppliedConf.getLong("spark.gcaware.groupCount", 100000L)
    val dateStart = suppliedConf.get("spark.gcaware.dateStart", "1995-01-01")
    val dateEnd = suppliedConf.get("spark.gcaware.dateEnd", "1996-01-01")
    val udfHeavy = suppliedConf.getBoolean("spark.gcaware.udfHeavy", false)
    val udfHeavyOps = suppliedConf.getInt("spark.gcaware.udfHeavyOps", 8)
    // Fraction of chronological date-partitions to scan on BOTH lineitem and
    // orders. 1.0 (default) == current full-scan behavior. Both TPC-H tables
    // here are DATE-partitioned (l_shipdate / o_orderdate), and o_orderkey is
    // uncorrelated with date, so an orderkey predicate prunes no row-groups --
    // the only lever that cuts real scan I/O is reading fewer partition dirs.
    val inputFraction = suppliedConf.getDouble("spark.gcaware.inputFraction", 1.0)

    require(inputFraction > 0.0 && inputFraction <= 1.0,
      "spark.gcaware.inputFraction must be in (0.0, 1.0]")
    require(arrayN > 0, "spark.gcaware.arrayN must be positive")
    require(udfHeavyOps > 0, "spark.gcaware.udfHeavyOps must be positive")
    require(groupCount > 0, "spark.gcaware.groupCount must be positive")
    require(dateStart.matches("\\d{4}-\\d{2}-\\d{2}"), "dateStart must be YYYY-MM-DD")
    require(dateEnd.matches("\\d{4}-\\d{2}-\\d{2}"), "dateEnd must be YYYY-MM-DD")
    require(dateStart < dateEnd, "dateStart must be before dateEnd")

    val builder = SparkSession.builder()
    if (!suppliedConf.contains("spark.app.name")) {
      suppliedConf.setAppName(s"heap-amplifier-sf$scale-n$arrayN-g$groupCount")
    }
    val spark = builder.config(suppliedConf).getOrCreate()

    var rows = -1L
    var durationMs = 0.0
    var exitCode = 0
    try {
      val (ordersDf, oSel, oTot) = readPrefix(spark, s"$dataLocation/orders", inputFraction)
      val (lineDf, lSel, lTot) = readPrefix(spark, s"$dataLocation/lineitem", inputFraction)
      ordersDf.createOrReplaceTempView("orders")
      lineDf.createOrReplaceTempView("lineitem")

      // In prefix mode the selected partitions are the early (chronological)
      // date range; the fixed date knob (default 1995-1996) would filter every
      // selected 1992+ row out, so the fraction's partition set IS the input
      // restriction and the l_shipdate WHERE is widened to a no-op window.
      val prefixMode = inputFraction < 1.0
      val effDateStart = if (prefixMode) "0001-01-01" else dateStart
      val effDateEnd = if (prefixMode) "9999-12-31" else dateEnd

      // Spark's Scala UDF is deliberately used instead of a native SQL
      // expression: it creates a JVM object/primitive-array boundary that
      // whole-stage codegen cannot inline through.
      spark.udf.register(
        "heap_array_amplify",
        if (udfHeavy) {
          (revenue: Double, groupKey: Long) =>
            HeapArrayAmplifier.scoreHeavy(revenue, groupKey, arrayN, udfHeavyOps)
        } else {
          (revenue: Double, groupKey: Long) =>
            HeapArrayAmplifier.score(revenue, groupKey, arrayN)
        })

      val sql = loadQuery()
        .replace("${DATA_LOCATION}", dataLocation)
        .replace("${GROUP_COUNT}", groupCount.toString)
        .replace("${DATE_START}", effDateStart)
        .replace("${DATE_END}", effDateEnd)

      println("=" * 54)
      println(s"Heap-array amplifier (TPC-H SF$scale)")
      println("=" * 54)
      println(s"Data Location: $dataLocation")
      println(s"Array N:       $arrayN doubles (~${arrayN * 8L} bytes/call)")
      println(s"UDF Mode:      ${if (udfHeavy) s"heavy (ops=$udfHeavyOps)" else "light"}")
      println(s"Group Count:   $groupCount")
      println(s"Ship Date:     [$effDateStart, $effDateEnd)")
      println(f"Input Frac:    $inputFraction%.4f  " +
        s"(orders ${fmtSel(oSel, oTot)}, lineitem ${fmtSel(lSel, lTot)} date-partitions)")
      println(s"App ID:        ${spark.sparkContext.applicationId}")

      val df = spark.sql(sql)
      println("--- Physical Plan before action ---")
      println(df.queryExecution.executedPlan.toString)
      val started = System.nanoTime()
      rows = df.collect().length.toLong
      durationMs = (System.nanoTime() - started) / 1e6
      println(s"AMPLIFIER_RESULT:rows=$rows:duration_ms=${"%.2f".format(durationMs)}")
    } catch {
      case t: Throwable =>
        exitCode = 1
        System.err.println(s"ERROR running $QueryName: ${t.getMessage}")
        t.printStackTrace()
    } finally {
      val appId = spark.sparkContext.applicationId
      println(s"QUERY_RESULT:tpch:$QueryName:$rows:${"%.2f".format(durationMs)}:$exitCode:$appId")
      println(s"AMPLIFIER_INFO:arrayN=$arrayN:bytes_per_call=${arrayN * 8L}:groups=$groupCount:dateStart=$dateStart:dateEnd=$dateEnd:udfHeavy=$udfHeavy:udfHeavyOps=$udfHeavyOps:inputFraction=$inputFraction")
      spark.stop()
    }
    if (exitCode != 0) System.exit(exitCode)
  }

  private def fmtSel(sel: Int, tot: Int): String =
    if (sel < 0) "all" else s"$sel/$tot"

  /**
   * Read a DATE-partitioned parquet table, scanning only the first
   * ceil(fraction * N) partition directories in chronological order (the
   * `col=YYYY-MM-DD` dir names sort lexicographically == chronologically).
   * This genuinely cuts scan bytes because Spark only opens the selected
   * part-files; `basePath` keeps the partition column resolvable.
   * fraction >= 1.0 == the original full-directory read (unchanged behavior).
   * Returns the frame plus (selected, total) partition counts for logging.
   *
   * ponytail: partition (date) pruning is the only I/O lever on this layout --
   * o_orderkey is uncorrelated with o_orderdate, so an orderkey predicate's
   * per-file min/max spans the whole keyspace and prunes no row-groups.
   */
  private def readPrefix(spark: SparkSession, dir: String, fraction: Double): (DataFrame, Int, Int) = {
    if (fraction >= 1.0) return (spark.read.parquet(dir), -1, -1)
    val base = new Path(dir)
    val fs = base.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val partDirs = fs.listStatus(base).filter(_.isDirectory).map(_.getPath).sortBy(_.getName)
    require(partDirs.nonEmpty, s"no partition sub-directories under $dir")
    val take = math.max(1, math.ceil(fraction * partDirs.length).toInt)
    val selected = partDirs.take(take).map(_.toString)
    (spark.read.option("basePath", dir).parquet(selected: _*), take, partDirs.length)
  }

  private def loadQuery(): String = {
    val in = Option(getClass.getResourceAsStream("/heap-amplifier.sql"))
      .getOrElse(throw new IllegalStateException("missing /heap-amplifier.sql resource"))
    try Source.fromInputStream(in, "UTF-8").mkString finally in.close()
  }
}

/** Allocation-heavy but deterministic JVM bytecode. */
object HeapArrayAmplifier {
  val VERSION = "heap-array-amplifier-v1"

  def score(revenue: Double, groupKey: Long, arrayN: Int): Double = {
    val values = new Array[Double](arrayN)
    var i = 0
    var sum = 0.0
    val seed = revenue + groupKey.toDouble * 0.000001
    while (i < values.length) {
      val value = seed + i.toDouble * 0.0000001
      values(i) = value * value + 1.0
      sum += values(i)
      i += 1
    }
    sum / values.length
  }

  // ---------------------------------------------------------------------------
  // Heavy variant (spark.gcaware.udfHeavy=true).
  //
  // Same allocation as `score` PLUS a big, branchy, HOT reduction so C2 actually
  // spends time compiling it. Each element passes through hand-rolled polynomial
  // approximations of exp/log/sin below; those are deliberately NOT java.lang.Math
  // calls (C2 intrinsifies Math.exp/sin/log to hardware ops or stubs, which would
  // erase the compile cost). Multiple accumulators + range branches make the IR
  // large and its register allocation/scheduling genuinely costly, while each
  // method stays well under HotSpot's 8000-bytecode HugeMethodLimit.
  //
  // ponytail: escape-analysis-safe because `values` is `new` at a RUNTIME size
  // (arrayN), written in one loop and then re-read across separate helper-method
  // calls in the reduction below; C2 cannot prove it non-escaping, so it stays a
  // real heap double[] and the humongous/arraylet allocation churn we measure is
  // preserved. ops>64-length rules out compile-time-constant scalar replacement.
  def scoreHeavy(revenue: Double, groupKey: Long, arrayN: Int, ops: Int): Double = {
    val values = new Array[Double](arrayN)
    var i = 0
    val seed = revenue + groupKey.toDouble * 0.000001
    while (i < values.length) {
      val value = seed + i.toDouble * 0.0000001
      values(i) = value * value + 1.0
      i += 1
    }

    // Multiple independent accumulators keep C2 from collapsing the loop body.
    var a0 = 0.0
    var a1 = 0.0
    var a2 = 0.0
    var a3 = 1.0e-9
    var pass = 0
    while (pass < ops) {
      var j = 0
      val phase = 0.5 + pass.toDouble * 0.125
      while (j < values.length) {
        val x = values(j) * 0.0009765625 + phase // scale into a sane range
        val e = polyExp(-x * x * 0.5)             // gaussian-ish bell
        val l = polyLog1p(x * 0.25)
        val s = polySin(x + a3)
        // Range branches select different combinations -> branchy compiled code.
        if (x > phase) {
          a0 += e * s + l
          a1 += e - l * s
        } else if (x > 0.0) {
          a0 += e * l - s
          a2 += s * s + e
        } else {
          a2 += l - e * s
          a3 = if (a3 < 1.0) a3 + e * 1.0e-3 else a3 * 0.999
        }
        values(j) = e + l * 0.5 - s * 0.25 // feed back so passes differ
        j += 1
      }
      pass += 1
    }
    (a0 - a1 + a2 + a3) / values.length
  }

  // Taylor series for exp on a reduced argument. No java.lang.Math -> no intrinsic.
  private def polyExp(x: Double): Double = {
    var term = 1.0
    var sum = 1.0
    var k = 1
    while (k <= 10) {
      term *= x / k.toDouble
      sum += term
      k += 1
    }
    sum
  }

  // Atanh-based log1p series, valid for the small scaled arguments above.
  private def polyLog1p(x: Double): Double = {
    val z = x / (2.0 + x)
    val z2 = z * z
    var term = z
    var sum = 0.0
    var k = 1
    while (k <= 9) {
      sum += term / k.toDouble
      term *= z2
      k += 2
    }
    2.0 * sum
  }

  // Minimax-ish odd Taylor series for sin. No java.lang.Math -> no intrinsic.
  private def polySin(x: Double): Double = {
    val x2 = x * x
    var term = x
    var sum = x
    var k = 1
    while (k <= 6) {
      val d = (2 * k) * (2 * k + 1)
      term *= -x2 / d.toDouble
      sum += term
      k += 1
    }
    sum
  }
}
