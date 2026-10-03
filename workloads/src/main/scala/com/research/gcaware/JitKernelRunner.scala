package com.research.gcaware

import org.apache.spark.{SparkConf, SparkContext}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

/**
 * Non-SQL executor probe for examining JVM compilation behaviour without
 * Catalyst, whole-stage code generation, external input, or floating point
 * library calls.  Each partition computes one XOR checksum and the driver
 * combines the (small) set of partition checksums with treeReduce.
 *
 * Positional arguments intentionally match the existing screening contract:
 *   <query> <scale> <dataLocation>
 * Only `baseline-v1` and `rules-v1` are valid queries; scale and dataLocation
 * are accepted for submit-script compatibility and are not read.
 */
object JitKernelRunner {
  private val Version = "rules-kernel-v1"
  private val SmallRecordsPerPartition = 4096L
  private val SmallPartitions = 3
  private val SmallRounds = 7

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: <query> <scale> <dataLocation>")
      System.exit(2)
    }

    val mode = args(0)
    if (mode != "baseline-v1" && mode != "rules-v1") {
      System.err.println(s"ERROR: jitkernel query must be baseline-v1 or rules-v1, got '$mode'")
      System.exit(2)
    }

    val scale = args(1)
    val dataLocation = args(2)
    val suppliedConf = new SparkConf()
    val recordsPerPartition = suppliedConf.getLong(
      "spark.gcaware.jitkernel.recordsPerPartition", 100000L)
    val rounds = suppliedConf.getInt("spark.gcaware.jitkernel.rounds", 1)
    val partitions = suppliedConf.getInt("spark.gcaware.jitkernel.partitions", 3)
    val verify = suppliedConf.getBoolean("spark.gcaware.jitkernel.verify", false)

    require(recordsPerPartition > 0L, "recordsPerPartition must be positive")
    require(rounds >= 0, "rounds must be non-negative")
    require(partitions > 0, "partitions must be positive")
    val totalRecords = Math.multiplyExact(recordsPerPartition, partitions.toLong)

    if (!suppliedConf.contains("spark.app.name")) {
      suppliedConf.setAppName(s"jitkernel-$mode")
    }
    val sc = SparkContext.getOrCreate(suppliedConf)
    var checksum = 0L
    var actionMs = 0L
    var exitCode = 0

    try {
      println(s"JITKERNEL_INPUT: mode=$mode scale_ignored=$scale dataLocation_ignored=$dataLocation " +
        s"records_per_partition=$recordsPerPartition rounds=$rounds partitions=$partitions verify=$verify")

      awaitCaptureGate(sc, suppliedConf)

      if (verify) verifySmallReference(sc, mode)

      val started = System.nanoTime()
      checksum = distributedChecksum(sc, mode, recordsPerPartition, rounds, partitions, totalRecords)
      actionMs = (System.nanoTime() - started) / 1000000L
    } catch {
      case t: Throwable =>
        exitCode = 1
        System.err.println(s"ERROR running jitkernel $mode: ${t.getMessage}")
        t.printStackTrace()
    } finally {
      val appId = sc.applicationId
      println(s"KERNEL_RESULT:version=$Version:mode=$mode:records=$recordsPerPartition:" +
        s"rounds=$rounds:partitions=$partitions:checksum=$checksum:action_ms=$actionMs:" +
        s"appid=$appId:exit_code=$exitCode")
      sc.stop()
    }
    if (exitCode != 0) System.exit(exitCode)
  }

  /**
   * Optional experiment-control barrier.  The driver writes READY after its
   * SparkContext exists but before it submits an action.  The external
   * collector snapshots the live JVMs and writes ACK through the shared logs
   * volume; only then may the timed action start.  It is off by default so it
   * never changes ordinary screening runs.
   */
  private def awaitCaptureGate(sc: SparkContext, conf: SparkConf): Unit = {
    if (!conf.getBoolean("spark.gcaware.captureGate.enabled", false)) return
    val directory = Paths.get(conf.get("spark.gcaware.captureGate.dir", "/var/spark-logs/heterojvm-gates"))
    val timeoutMs = conf.getLong("spark.gcaware.captureGate.timeoutMs", 180000L)
    require(directory.isAbsolute, "spark.gcaware.captureGate.dir must be absolute")
    require(timeoutMs > 0L, "spark.gcaware.captureGate.timeoutMs must be positive")
    Files.createDirectories(directory)
    val appId = sc.applicationId
    val ready = directory.resolve(s"$appId.ready")
    val ack = directory.resolve(s"$appId.ack")
    Files.writeString(ready, s"appid=$appId\nready_at_ms=${System.currentTimeMillis()}\n", StandardCharsets.UTF_8,
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    println(s"WORKLOAD_READY:appid=$appId:gate_dir=$directory")
    Console.out.flush()
    val deadline = System.nanoTime() + timeoutMs * 1000000L
    while (!Files.isRegularFile(ack) && System.nanoTime() < deadline) Thread.sleep(100L)
    if (!Files.isRegularFile(ack)) {
      throw new IllegalStateException(s"capture gate timed out waiting for $ack")
    }
    println(s"WORKLOAD_GATE_ACK:appid=$appId")
  }

  private def distributedChecksum(
      sc: SparkContext,
      mode: String,
      recordsPerPartition: Long,
      rounds: Int,
      partitions: Int,
      totalRecords: Long): Long = {
    sc.range(0L, totalRecords, 1L, partitions)
      .mapPartitions { values =>
        var local = 0L
        while (values.hasNext) {
          val value = values.next()
          local ^= RulesKernelV1.evaluate(mode, value, rounds)
        }
        Iterator.single(local)
      }
      .treeReduce((left, right) => left ^ right, depth = 2)
  }

  /** A small local-vs-distributed invariant used only when verify=true. */
  private def verifySmallReference(sc: SparkContext, mode: String): Unit = {
    val total = SmallRecordsPerPartition * SmallPartitions
    val expected = localChecksum(mode, total, SmallRounds)
    val observed = distributedChecksum(
      sc, mode, SmallRecordsPerPartition, SmallRounds, SmallPartitions, total)
    if (observed != expected) {
      throw new IllegalStateException(
        s"jitkernel verification failed: expected checksum=$expected observed=$observed")
    }
    println(s"KERNEL_VERIFY:version=$Version:mode=$mode:records=$total:rounds=$SmallRounds:checksum=$observed")
  }

  private def localChecksum(mode: String, records: Long, rounds: Int): Long = {
    var value = 0L
    var checksum = 0L
    while (value < records) {
      checksum ^= RulesKernelV1.evaluate(mode, value, rounds)
      value += 1L
    }
    checksum
  }
}

/**
 * Stable primitive-only helper graph used by JitKernelRunner.  The lookup
 * table is allocated once at class initialization; evaluate and its callees
 * allocate no objects per input record.
 */
object RulesKernelV1 {
  private val Table: Array[Long] = Array(
    0x243f6a8885a308d3L, -4942790177534073029L,
    -1479977612322866047L, 0x13198a2e03707344L,
    -6584101470606114471L, -3786177312176994800L,
    0x3f84d5b5b5470917L, -4804623566734708680L)

  private val Seed = -7046029254386353131L
  private val Mul1 = -4658895280553007687L
  private val Mul2 = -7723592293110705685L

  def evaluate(mode: String, value: Long, rounds: Int): Long = {
    if (mode == "baseline-v1") baseline(value) else rules(value, rounds)
  }

  /** Same input/checksum path as rules-v1, with no rules-loop work. */
  private def baseline(value: Long): Long = finalizeState(value ^ Seed)

  private def rules(value: Long, rounds: Int): Long = {
    var state = initialize(value)
    var round = 0
    while (round < rounds) {
      state = transition(state, value, round)
      round += 1
    }
    finalizeState(state)
  }

  private def initialize(value: Long): Long =
    scramble(value ^ Seed)

  private def transition(state: Long, value: Long, round: Int): Long = {
    val selector = ((state >>> 61) ^ round.toLong).toInt & 7
    val tableValue = Table(selector)
    val input = state ^ (value + tableValue) ^ (round.toLong * Seed)
    val mixed = scramble(input)
    if ((mixed & 1L) == 0L) {
      java.lang.Long.rotateLeft(mixed ^ tableValue, (selector + 11) & 63)
    } else {
      java.lang.Long.rotateRight(mixed + tableValue, (selector + 17) & 63) ^ (mixed >>> 7)
    }
  }

  private def scramble(input: Long): Long = {
    var z = input
    z = (z ^ (z >>> 30)) * Mul1
    z = (z ^ (z >>> 27)) * Mul2
    z ^ (z >>> 31)
  }

  private def finalizeState(state: Long): Long =
    scramble(state ^ java.lang.Long.rotateLeft(state, 23))
}
