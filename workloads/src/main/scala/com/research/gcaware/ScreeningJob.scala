package com.research.gcaware

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * Cluster-mode GC-screening job, the spark-submit counterpart of the PySpark
 * `screening.py` / `tpc_pyspark.py` logic. Executor sizing, AQE, broadcast
 * thresholds and GC flags are supplied by `run-screening.sh` via `--conf`; this
 * job only registers the benchmark tables, runs one named query, times it, and
 * prints a machine-parseable line.
 *
 * Args (positional, from the shell script): <query> <scale> <dataLocation>
 *   query        e.g. "q9" -> bundled resource /<benchmark>/q9.sql
 *   scale        scale factor (informational; dataLocation already encodes it)
 *   dataLocation parquet root, e.g. s3a://spark-obj-storage/tpcds-scale-100
 */
object ScreeningJob {

  // Parquet table directory names (ports tpc_pyspark.TPCDS_TABLES / TPCH_TABLES).
  private val TPCDS_TABLES = Seq(
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item", "promotion",
    "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
    "warehouse", "web_page", "web_returns", "web_sales", "web_site")

  private val TPCH_TABLES = Seq(
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region",
    "supplier")

  private def tableNames(benchmark: String): Seq[String] = benchmark.toLowerCase match {
    case "tpcds" => TPCDS_TABLES
    case "tpch"  => TPCH_TABLES
    case other   => throw new IllegalArgumentException(s"benchmark must be tpcds or tpch, got '$other'")
  }

  /** Load a bundled query, e.g. ("tpcds", "q9") -> resource /tpcds/q9.sql.
   *  Falls back to the /udf/<bench>/ overlay for UDF query variants: build.sh
   *  wipes and re-copies /<bench>/ from the canonical Spark test resources on
   *  every build, but never touches /udf/, so variants there survive rebuilds.
   *  Run a variant by passing its name, e.g. query "q3-udf" -> /udf/tpch/q3-udf.sql. */
  private def loadSql(benchmark: String, query: String): String = {
    val name = if (query.endsWith(".sql")) query else s"$query.sql"
    val b = benchmark.toLowerCase
    val candidates = Seq(s"/$b/$name", s"/udf/$b/$name")
    val in = candidates.iterator
      .flatMap(p => Option(getClass.getResourceAsStream(p)))
      .nextOption().getOrElse(
        throw new IllegalArgumentException(
          s"Query resource not found on classpath, tried: ${candidates.mkString(", ")}"))
    try Source.fromInputStream(in, "UTF-8").mkString finally in.close()
  }

  // Phase boundaries for the pre-query decomposition (wall-clock epoch ms, driver stdout).
  private def phaseMark(name: String): Unit =
    println(s"PHASE_MARK:$name:${System.currentTimeMillis()}")

  def run(benchmark: String, args: Array[String]): Unit = {
    phaseMark("main_entry")
    if (args.length < 3) {
      System.err.println("Usage: <query> <scale> <dataLocation>")
      System.exit(2)
    }
    val query = args(0)
    val scale = args(1)
    val dataLocation = args(2).stripSuffix("/")
    val runId = s"screen_${benchmark}_${query}"

    // Inherit all confs set by spark-submit (sizing, AQE, thresholds, GC, S3A).
    // Respect the app name passed via --name / --conf spark.app.name (run-screening.sh
    // sets it to the informative {bench}-{query}-{sf}-{gc}-{aqe}-{heap}-{cores} form);
    // fall back to runId only when no name was supplied.
    val builder = SparkSession.builder()
    if (!new SparkConf().contains("spark.app.name")) builder.appName(runId)
    val spark = builder.getOrCreate()

    // Compute-heavy pricing UDF (JIT hot-loop workload). Harmless if a query
    // doesn't call it; only the *-udf query variants do. Intensity via
    // -Dspark.gcaware.udf.iters (conf). udfCalls counts invocations for logging.
    val udfIters = spark.sparkContext.getConf.getInt("spark.gcaware.udf.iters", 256)
    val countUdfCalls = spark.sparkContext.getConf.getBoolean("spark.gcaware.udf.countCalls", true)
    val udfCalls = PriceUdf.register(spark, udfIters, countUdfCalls)
    phaseMark("session_ready")

    val line = "=" * 46
    var rows = -1L
    var durationMs = 0.0
    var exitCode = 0
    try {
      tableNames(benchmark).foreach { t =>
        spark.read.parquet(s"$dataLocation/$t").createOrReplaceTempView(t)
      }
      val sql = loadSql(benchmark, query)
      phaseMark("views_ready")

      println(line)
      println(s"GC Screening (cluster): $query ($benchmark SF$scale)")
      println(line)
      println(s"  Data Location: $dataLocation")
      println(s"  Spark Version: ${spark.version}")
      println(s"  App ID:        ${spark.sparkContext.applicationId}")
      println(line)

      val planDf = spark.sql(sql)
      println(s"--- Logical Plan ($query) ---")
      println(planDf.queryExecution.analyzed.toString)

      def materializeRows(df: org.apache.spark.sql.DataFrame): Long = {
        if (query.contains("-udf")) df.collect().length.toLong else df.count()
      }

      // Optional JIT warmup: re-execute the full query N times untimed so the
      // executors compile the hot path (incl. the UDF) to top tier BEFORE we
      // measure. Each pass re-runs the whole plan; JIT state persists across
      // passes within this app's executors. Opt-in via -Dspark.gcaware.warmup=<n>
      // (0 = off). Cache/SCC state is an explicit external experiment factor;
      // every run must record whether it is empty, preserved, or same-JVM warm.
      val warmupPasses = spark.sparkContext.getConf.getInt("spark.gcaware.warmup", 0)
      var wp = 0
      while (wp < warmupPasses) {
        val warmupDf = spark.sql(sql)
        val wrows = materializeRows(warmupDf)
        println(s"WARMUP: pass ${wp + 1}/$warmupPasses rows=$wrows")
        wp += 1
      }

      val timedDf = spark.sql(sql)
      phaseMark("query_start")
      val start = System.nanoTime()
      rows = materializeRows(timedDf)
      durationMs = (System.nanoTime() - start) / 1e6
      phaseMark("query_end")

      println(s"--- Physical Plan after AQE ($query) ---")
      println(timedDf.queryExecution.executedPlan.toString)
      println(s"Query $query completed: rows=$rows, duration=${"%.2f".format(durationMs)} ms")
    } catch {
      case e: Throwable =>
        exitCode = 1
        System.err.println(s"ERROR running $query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      val appId = spark.sparkContext.applicationId
      // Machine-parseable (the shell script emits the full RESULT: line).
      println(s"QUERY_RESULT:$benchmark:$query:$rows:${"%.2f".format(durationMs)}:$exitCode:$appId")
      // UDF provenance for run artifacts: which UDF version + intensity produced this
      // result, and how many rows hit it (0 = query didn't use the UDF). Aggregate
      // only; never logged per-row.
      val callCount = if (countUdfCalls) udfCalls.value.toString else "disabled"
      println(s"UDF_INFO:price_score:${PriceUdf.VERSION}:iters=$udfIters:calls=$callCount")
      spark.stop()
      phaseMark("spark_stopped")
    }
    if (exitCode != 0) System.exit(exitCode)
  }
}

/**
 * Compute-heavy pricing UDF: a CPU-bound hot loop over the real TPC-H price
 * columns (l_extendedprice, l_discount). Purpose is to give the JIT a tight,
 * data-dependent inner loop to compile — the lever for comparing OpenJ9's JIT
 * against HotSpot C2 on a UDF-heavy stage. Not a GC/allocation workload.
 *
 * Bump VERSION on any change to the kernel so run artifacts stay traceable to
 * exactly which UDF produced a result.
 */
object PriceUdf {
  val VERSION = "priceScore-logistic-v1"
  val VERSION_COMPLEX = "priceScore-branchy-v1"

  /**
   * Deterministic, pure, allocation-free kernel. Seeds a logistic map from the
   * real line revenue and iterates it `iters` times, then scales revenue by the
   * result. Data-dependent (can't be hoisted or strength-reduced), bounded in
   * (0,1), no I/O, no randomness, no time — same inputs always give the same
   * output, so results are stable across runs and JVMs.
   */
  def score(ep: Double, disc: Double, iters: Int): Double = {
    val revenue = ep * (1.0 - disc)
    var x = (math.abs(revenue) % 1000.0) / 1000.0
    if (x <= 0.0 || x >= 1.0) x = 0.5
    var i = 0
    while (i < iters) { x = 3.9 * x * (1.0 - x); i += 1 }
    revenue * x
  }

  /**
   * Register `price_score` and optionally return an accumulator counting invocations.
   * Disable the counter for timing runs where per-call instrumentation would
   * become part of the hot path.
   */
  /**
   * Complex allocation-free kernel: data-dependent branches + transcendental
   * calls (sqrt/log/cbrt/sin) per iteration. Harder for a JIT to optimize than
   * the plain logistic loop (unpredictable branch, several intrinsics), so it
   * can expose JIT-quality differences between HotSpot C2 and OpenJ9. Still
   * deterministic, no I/O, no allocation. Selected with
   * -Dspark.gcaware.udf.kernel=complex. Its hot method name is `scoreComplex`,
   * so force-scorching must target PriceUdf$.scoreComplex(DDI)D for this kernel.
   */
  def scoreComplex(ep: Double, disc: Double, iters: Int): Double = {
    val revenue = ep * (1.0 - disc)
    var x = (math.abs(revenue) % 1000.0) / 1000.0
    if (x <= 0.0 || x >= 1.0) x = 0.5
    var acc = 0.0
    var i = 0
    while (i < iters) {
      if (x < 0.5) x = math.sqrt(x + 0.1) - math.log1p(x)
      else         x = math.cbrt(x) * 0.9 + math.sin(x)
      if (x < 0.0) x = -x
      if (x >= 1.0) x = x - math.floor(x)
      acc += x
      i += 1
    }
    revenue * (acc / math.max(iters, 1))
  }

  def register(
      spark: SparkSession,
      iters: Int,
      countCalls: Boolean): org.apache.spark.util.LongAccumulator = {
    val kernel = spark.sparkContext.getConf.get("spark.gcaware.udf.kernel", "simple")
    val complex = kernel == "complex"
    val calls = spark.sparkContext.longAccumulator("price_score_calls")
    spark.udf.register("price_score", (ep: Double, disc: Double) => {
      if (countCalls) calls.add(1L)
      if (complex) scoreComplex(ep, disc, iters) else score(ep, disc, iters)
    })
    val ver = if (complex) VERSION_COMPLEX else VERSION
    println(
      s"UDF_REGISTERED: name=price_score kernel=$kernel version=$ver " +
        s"iters=$iters countCalls=$countCalls")
    calls
  }
}

object TpcdsQueryRunner {
  def main(args: Array[String]): Unit = ScreeningJob.run("tpcds", args)
}

object TpchQueryRunner {
  def main(args: Array[String]): Unit = ScreeningJob.run("tpch", args)
}
