package com.research.gcaware

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._

/**
 * UDFBench-on-JVM query runner — the Scala/native-UDF port of the PySpark
 * UDFBench engine, so the identical GC-study jar runs on HotSpot and OpenJ9.
 *
 * Same contract as ScreeningJob: <query> <scale> <dataLocation> [filesDir],
 * register the benchmark tables as temp views, run one bundled query, time it,
 * print a machine-parseable QUERY_RESULT line. Executor sizing / GC / AQE come
 * from run-screening.sh via --conf; nothing experiment-shaped is hard-coded here.
 *
 * dataLocation is the parquet root, e.g. file:///mnt/bench/udfbench/small/parquet
 * (tables live at <dataLocation>/<table>/<table>.parquet).
 *
 * filesDir is where the external inputs (crossref.txt/.xml, arxiv.csv, pubmed*.txt)
 * live, resolved (in order): 4th CLI arg, conf spark.gcaware.udfbench.filesDir, or
 * default /mnt/bench/udfbench/files/<scale>. The file-reading UDFs read from there
 * with plain JVM IO on the executor-local mount (matching the Python open()).
 */
object UdfbenchQueryRunner {

  /** table -> column names, ported verbatim from pyspark_load.py's .toDF(...).
   *  Tables not listed keep their parquet column names (unused by ported queries). */
  private val TABLE_COLUMNS: Map[String, Seq[String]] = Map(
    "artifacts" -> Seq("id", "title", "publisher", "journal", "date", "year",
      "access_mode", "embargo_end_date", "delayed", "authors", "source",
      "abstract", "type", "peer_reviewed", "green", "gold"),
    "projects" -> Seq("id", "acronym", "title", "funder", "fundingstring",
      "funding_lvl0", "funding_lvl1", "funding_lvl2", "ec39", "type", "startdate",
      "enddate", "start_year", "end_year", "duration", "haspubs", "numpubs",
      "daysforlastpub", "delayedpubs", "callidentifier", "code", "totalcost",
      "fundedamount", "currency"),
    "projects_artifacts" -> Seq("projectid", "artifactid", "provenance"),
    "artifact_authorlists" -> Seq("artifactid", "authorlist"),
    "artifact_citations" -> Seq("artifactid", "target", "citcount"),
    "artifact_abstracts" -> Seq("artifactid", "abstract"),
    "artifact_authors" -> Seq("artifactid", "affiliation", "fullname", "name",
      "surname", "rank", "authorid"),
    "views_stats" -> Seq("date", "artifactid", "source", "repository_id", "count"))

  private val ALL_TABLES: Seq[String] = TABLE_COLUMNS.keys.toSeq ++
    Seq("artifact_charges", "project_artifactcount")

  private def loadSql(query: String): String = {
    val name = if (query.endsWith(".sql")) query else s"$query.sql"
    val in = Option(getClass.getResourceAsStream(s"/udfbench/$name")).getOrElse(
      throw new IllegalArgumentException(s"Query resource not found: /udfbench/$name"))
    try Source.fromInputStream(in, "UTF-8").mkString.trim.stripSuffix(";")
    finally in.close()
  }

  private def registerTable(spark: SparkSession, dataLocation: String, t: String): Unit = {
    val df = spark.read.parquet(s"$dataLocation/$t")
    val named = TABLE_COLUMNS.get(t) match {
      case Some(cols) if cols.length == df.columns.length => df.toDF(cols: _*)
      case Some(cols) =>
        throw new IllegalStateException(
          s"table $t: parquet has ${df.columns.length} cols, expected ${cols.length}")
      case None => df
    }
    named.createOrReplaceTempView(t)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: <query> <scale> <dataLocation> [filesDir]")
      System.exit(2)
    }
    val query = args(0)
    val scale = args(1)
    val dataLocation = args(2).stripSuffix("/")

    val builder = SparkSession.builder()
    if (!new SparkConf().contains("spark.app.name")) builder.appName(s"udfbench_$query")
    val spark = builder.getOrCreate()

    // filesDir: 4th arg > conf > default. Strip any file:// scheme (UDFs use JVM IO).
    val filesDir = (if (args.length >= 4 && args(3).nonEmpty) args(3)
      else spark.sparkContext.getConf.getOption("spark.gcaware.udfbench.filesDir")
        .getOrElse(s"/mnt/bench/udfbench/files/$scale"))
      .stripSuffix("/").stripPrefix("file://")

    UdfbenchUdfs.registerAll(spark, filesDir)

    val line = "=" * 46
    var rows = -1L
    var durationMs = 0.0
    var exitCode = 0
    try {
      ALL_TABLES.foreach { t =>
        try registerTable(spark, dataLocation, t)
        catch { case _: Throwable => /* table dir absent — skip */ }
      }
      val sql = loadSql(query)

      println(line)
      println(s"UDFBench (cluster): $query (scale=$scale)")
      println(s"  Data Location: $dataLocation")
      println(s"  Files Dir:     $filesDir")
      println(s"  App ID:        ${spark.sparkContext.applicationId}")
      println(line)

      def materialize(df: DataFrame): Long = df.count()

      val warmupPasses = spark.sparkContext.getConf.getInt("spark.gcaware.warmup", 0)
      var wp = 0
      while (wp < warmupPasses) {
        println(s"WARMUP: pass ${wp + 1}/$warmupPasses rows=${materialize(spark.sql(sql))}")
        wp += 1
      }

      val timedDf = spark.sql(sql)
      val start = System.nanoTime()
      rows = materialize(timedDf)
      durationMs = (System.nanoTime() - start) / 1e6
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
      println(s"QUERY_RESULT:udfbench:$query:$rows:${"%.2f".format(durationMs)}:$exitCode:$appId")
      spark.stop()
    }
    if (exitCode != 0) System.exit(exitCode)
  }
}

// ---- Struct return types for the array-returning (ex-UDTF) and struct UDFs.
// Top-level case classes so Spark's ScalaReflection can derive their schema. ----
/** extractfromdate output row (q2). Integer fields so null/-1 both encode. */
case class Ymd(year: java.lang.Integer, month: java.lang.Integer, day: java.lang.Integer)
/** jsonparse / extractkeys output row (q13, q15). */
case class DoiPair(publicationdoi: String, fundinginfo: String)
/** 3-column file record (q7: id, citations, authors). */
case class Cols3(c1: String, c2: String, c3: String)
/** 2-column file record (q18: id, abstract). */
case class Cols2(c1: String, c2: String)
case class KCluster(clusterid: Int, id: String, points: Double)

/**
 * UDFBench UDFs as Scala native functions, ported from the udfs/scalar,
 * udfs/aggregate and udfs/table Python modules.
 *
 * The Python @udtf table functions are NOT reproduced as generators; each is
 * ported to a Scala UDF that returns an ARRAY (or a struct), and the query SQL
 * calls it with LATERAL VIEW explode / struct-field access — the idiomatic
 * Spark-SQL shape. Null/parse-error semantics match the Python faithfully.
 */
object UdfbenchUdfs {
  private val mapper = new ObjectMapper()

  private def parseStrList(s: String): mutable.Buffer[String] = {
    val n = mapper.readTree(s)
    n.elements().asScala.map(_.asText()).toBuffer
  }
  private def dumps(xs: Seq[String]): String = mapper.writeValueAsString(xs.asJava)
  private def nodeText(n: JsonNode): String = if (n == null || n.isNull) null else n.asText()

  // U1 addnoise: gaussian noise (std 2). `if val:` -> 0/null falsy -> None.
  private val noiseRng = new java.util.Random()
  def addnoise(v: java.lang.Double): java.lang.Double =
    if (v == null || v == 0.0) null
    else try java.lang.Double.valueOf(v + noiseRng.nextGaussian() * 2.0)
    catch { case _: Throwable => null }

  // U2 clean: json list -> lower, drop words<=2 chars, sort tokens, sort list.
  def clean(v: String): String = {
    if (v == null || v.isEmpty) return null
    try {
      val names = parseStrList(v).map { name =>
        val kept = name.toLowerCase.split(" ").filter(_.length > 2)
        kept.sorted.mkString(" ")
      }
      dumps(names.sorted.toSeq)
    } catch { case _: Throwable => "[]" }
  }

  // U3 cleandate: normalise dirty dates; only 1-2 separators accepted.
  def cleandate(pubdate: String): String = {
    if (pubdate == null || pubdate.isEmpty) return null
    try {
      if (pubdate.contains("-")) {
        val p = pubdate.split("-", -1)
        pubdate.count(_ == '-') match {
          case 1 => p(0) + "/" + p(1) + "/01"
          case 2 => p(0) + "/" + p(1) + "/" + p(2)
          case _ => null
        }
      } else if (pubdate.contains("/")) {
        val p = pubdate.split("/", -1)
        pubdate.count(_ == '/') match {
          case 1 => p(0) + "/" + p(1) + "/01"
          case 2 => p(0) + "-" + p(1) + "-" + p(2)
          case _ => null
        }
      } else null
    } catch { case _: Throwable => null }
  }

  // U4 converttoeuro: fixed rate table; unknown currency -> 0.0 (except branch).
  private val euro = Map("EUR" -> 1.00, "" -> 1.00, "NOK" -> 11.59, "AUD" -> 1.63,
    "CAD" -> 1.44, "$" -> 1.09, "USD" -> 1.09, "GBP" -> 0.85, "CHF" -> 0.98,
    "ZAR" -> 20.41, "SGD" -> 1.47, "INR" -> 89.61)
  def converttoeuro(x: java.lang.Double, y: String): java.lang.Double =
    if (x == null || y == null) null
    else euro.get(y) match { case Some(r) => x / r; case None => 0.0 }

  // U5/U6/U8/U9/U11 funder::class::projectid splitters.
  def extractclass(p: String): String =
    if (p == null || p.isEmpty) null else try p.split("::")(1) catch { case _: Throwable => null }
  def extractcode(p: String): String =
    if (p == null || p.isEmpty) null else try p.split("::")(2) catch { case _: Throwable => null }
  def extractid(p: String): String =
    if (p == null || p.isEmpty) null else try p.split("::")(2) catch { case _: Throwable => null }
  def extractfunder(p: String): String =
    if (p == null || p.isEmpty) null
    else try if (p.contains("::")) p.split("::")(0) else null catch { case _: Throwable => null }

  private val projIdPat = java.util.regex.Pattern.compile("(?<!\\d)[0-9]{6}(?!\\d)")
  def extractprojectid(in: String): String = {
    if (in == null || in.isEmpty) return null
    val m = projIdPat.matcher(in)
    if (m.find()) m.group() else ""
  }

  // U7/U10/U12 date parts: int(...) parse failure -> -1.
  def extractyear(a: String): java.lang.Integer =
    if (a == null || a.isEmpty) null else try Integer.valueOf(a.substring(0, a.indexOf('-'))) catch { case _: Throwable => -1 }
  def extractmonth(a: String): java.lang.Integer =
    if (a == null || a.isEmpty) null else try Integer.valueOf(a.substring(a.indexOf('-') + 1, a.lastIndexOf('-'))) catch { case _: Throwable => -1 }
  def extractday(a: String): java.lang.Integer =
    if (a == null || a.isEmpty) null else try Integer.valueOf(a.substring(a.lastIndexOf('-') + 1)) catch { case _: Throwable => -1 }

  // U30 extractfromdate (ex-UDTF, but 1:1): year/month/day parsed atomically —
  // any part failing sends all three to -1 (matches the single try/except).
  def extractfromdate(a: String): Ymd = {
    if (a == null || a.isEmpty) return Ymd(null, null, null)
    try Ymd(
      Integer.valueOf(a.substring(0, a.indexOf('-'))),
      Integer.valueOf(a.substring(a.indexOf('-') + 1, a.lastIndexOf('-'))),
      Integer.valueOf(a.substring(a.lastIndexOf('-') + 1)))
    catch { case _: Throwable => Ymd(-1, -1, -1) }
  }

  // U14 frequentterms: N most-frequent lowercased tokens.
  def frequentterms(in: String, n: java.lang.Integer): String = {
    if (in == null || in.isEmpty) return null
    try {
      val counts = mutable.LinkedHashMap[String, Int]()
      in.split("\\s+").filter(_.nonEmpty).foreach { w =>
        val lw = w.toLowerCase; counts(lw) = counts.getOrElse(lw, 0) + 1
      }
      counts.toSeq.sortBy(-_._2).take(n).map(_._1).mkString(" ")
    } catch { case _: Throwable => "" }
  }

  // U15 jaccard: |A∩B| / |A∪B| over two json lists.
  def jaccard(a: String, b: String): java.lang.Double = {
    if (a == null || b == null) return null
    try {
      def toSet(s: String): Set[String] =
        mapper.readTree(s).elements().asScala.map(_.toString).toSet
      val rset = toSet(a); val sset = toSet(b)
      java.lang.Double.valueOf((rset intersect sset).size.toDouble / (rset union sset).size)
    } catch { case _: Throwable => null }
  }

  // U16 jpack: whitespace tokens -> json list.
  def jpack(in: String): String =
    if (in == null || in.isEmpty) null
    else try dumps(in.split("\\s+").filter(_.nonEmpty).toSeq) catch { case _: Throwable => "" }

  // U17 jsoncount: length of a json list (no null guard in Python -> NPE -> None).
  def jsoncount(jval: String): java.lang.Long =
    try if (jval.charAt(0) == '[') mapper.readTree(jval).size().toLong else 1L
    catch { case _: Throwable => null }

  // U18 jsonparse (registered as jsonparse_q14): value for key1; list -> first elem.
  def jsonparse(json: String, key1: String): String = {
    try {
      val data = mapper.readTree(json)
      val node: JsonNode =
        if (data.isArray) { val it = data.elements(); if (it.hasNext) it.next().get(key1) else null }
        else if (data.isObject) data.get(key1)
        else null
      nodeText(node)
    } catch { case _: Throwable => null }
  }

  // U31 jsonparse (table, 1:1): parse a json dict -> (key1, key2). Non-dict/err -> (null,null).
  def jsonparsePair(json: String, key1: String, key2: String): DoiPair = {
    try {
      val data = mapper.readTree(json)
      if (data.isObject) DoiPair(nodeText(data.get(key1)), nodeText(data.get(key2)))
      else DoiPair(null, null)
    } catch { case _: Throwable => DoiPair(null, null) }
  }

  // U33 extractkeys (table): json dict -> one pair; json list -> a pair per item.
  def extractkeys(jval: String, key1: String, key2: String): Array[DoiPair] = {
    try {
      val data = mapper.readTree(jval)
      if (data.isArray)
        data.elements().asScala.map(it => DoiPair(nodeText(it.get(key1)), nodeText(it.get(key2)))).toArray
      else if (data.isObject) Array(DoiPair(nodeText(data.get(key1)), nodeText(data.get(key2))))
      else Array(DoiPair(null, null))
    } catch { case _: Throwable => Array(DoiPair(null, null)) }
  }

  // U32 combinations (table): json list -> every N-combination as a json list string.
  // Reused by q9 and q16. Parse error -> ["[]"] (matches the except's single '[]' yield).
  def combinations(vals: String, n: Int): Array[String] = {
    try {
      val items = parseStrList(vals).toIndexedSeq
      items.combinations(n).map(c => dumps(c)).toArray
    } catch { case _: Throwable => Array("[]") }
  }

  // U19 jsort: sort a json list. ponytail: assumes string elements (the only use).
  def jsort(jval: String): String =
    try dumps(parseStrList(jval).sorted.toSeq) catch { case _: Throwable => "[]" }

  // U20 jsortvalues: sort the space-separated tokens inside each list value.
  def jsortvalues(jval: String): String =
    try dumps(parseStrList(jval).map(n => n.split(" ").sorted.mkString(" ")).toSeq)
    catch { case _: Throwable => "[]" }

  // U21 keywords: unicode word tokens, drop bare '.', join.
  private val kwPat = java.util.regex.Pattern.compile("([\\d.]+\\b|\\w+)", java.util.regex.Pattern.UNICODE_CHARACTER_CLASS)
  def keywords(in: String): String = {
    if (in == null || in.isEmpty) return null
    try {
      val m = kwPat.matcher(in); val sb = new mutable.ArrayBuffer[String]()
      while (m.find()) { val x = m.group(); if (x != ".") sb += x }
      sb.mkString(" ")
    } catch { case _: Throwable => "" }
  }

  // U22 log_10: log10; any failure (incl. null) -> 0.0.
  def log_10(x: java.lang.Double): java.lang.Double =
    try if (x == null) 0.0 else { val r = math.log10(x); if (r.isNaN || r.isInfinite) 0.0 else r }
    catch { case _: Throwable => 0.0 }

  // U23 lowerize.
  def lowerize(v: String): String =
    if (v == null || v.isEmpty) null else try v.toLowerCase catch { case _: Throwable => "" }

  // U24 removeshortterms: drop tokens < 3 chars inside each list value.
  def removeshortterms(jval: String): String =
    try dumps(parseStrList(jval).map(n => n.split(" ").filter(_.length > 2).mkString(" ")).toSeq)
    catch { case _: Throwable => "[]" }

  // U13 filterstopwords: drop '' and tokens whose (first-char-lowercased) form is a
  // stopword. Stopword set (~3900 entries) is bundled as a resource to keep this
  // class-file small. ponytail: matches the Python `k[0].lower()+k[1:]` key.
  private lazy val stopwords: Set[String] = {
    val in = Option(getClass.getResourceAsStream("/udfbench/stopwords.txt"))
      .getOrElse(throw new IllegalStateException("missing /udfbench/stopwords.txt"))
    try Source.fromInputStream(in, "UTF-8").getLines().toSet finally in.close()
  }
  private def swKey(k: String): String =
    if (k.isEmpty) k else k.charAt(0).toLower.toString + k.substring(1)
  def filterstopwords(words: String): String = {
    if (words == null) return null
    try words.split(" ", -1).filter(k => k != "" && !stopwords.contains(swKey(k))).mkString(" ")
    catch { case _: Throwable => "" }
  }

  // U25 stem: Porter2 (Snowball English), ported from pyporter2. `''` on error, None on null.
  def stem(in: String): String = {
    if (in == null) return null
    try in.split("\\s+").filter(_.nonEmpty).map(Porter2.stem).mkString(" ")
    catch { case _: Throwable => "" }
  }

  // ---------- external-file readers (ex-UDTF `file`/`xmlparser`) ----------
  // Read executor-local files under filesDir with plain JVM IO, matching Python open().

  private val naSet = Set("", "NA", "N/A", "#N/A", "NaN", "nan", "null", "NULL")

  private def readLines(dir: String, name: String): Seq[String] =
    Files.readAllLines(Paths.get(dir, name), StandardCharsets.UTF_8).asScala.toSeq

  private def readAll(dir: String, name: String): String =
    new String(Files.readAllBytes(Paths.get(dir, name)), StandardCharsets.UTF_8)

  /** file(...,'json'): '['-prefixed -> whole-file array of dicts; else one dict per line.
   *  Each record -> the dict's values in insertion order. */
  private def readJsonRecords(dir: String, name: String): Seq[Seq[String]] = {
    val content = readAll(dir, name)
    val firstCh = content.dropWhile(_.isWhitespace).headOption.getOrElse(' ')
    def values(node: JsonNode): Seq[String] =
      node.fields().asScala.map(e => nodeText(e.getValue)).toSeq
    if (firstCh == '[') {
      val arr = mapper.readTree(content)
      arr.elements().asScala.filter(_.isObject).map(values).toSeq
    } else {
      content.linesIterator.filter(_.nonEmpty).map { l =>
        val n = mapper.readTree(l); if (n.isObject) values(n) else Seq.empty[String]
      }.toSeq
    }
  }

  /** file(...,'csv'): pandas read_csv(header=None)-style records; na-like fields -> null. */
  private def readCsvRecords(dir: String, name: String): Seq[Array[String]] = {
    val content = readAll(dir, name)
    val recs = mutable.ArrayBuffer[Array[String]]()
    val fields = mutable.ArrayBuffer[String]()
    val cur = new StringBuilder
    var quoted = false // whether current field was ever quoted
    var inQ = false
    var i = 0
    val nlen = content.length
    def endField(): Unit = {
      val raw = cur.toString()
      fields += (if (!quoted && naSet.contains(raw)) null else raw)
      cur.setLength(0); quoted = false
    }
    def endRecord(): Unit = { endField(); recs += fields.toArray; fields.clear() }
    while (i < nlen) {
      val c = content.charAt(i)
      if (inQ) {
        if (c == '"') {
          if (i + 1 < nlen && content.charAt(i + 1) == '"') { cur.append('"'); i += 1 }
          else inQ = false
        } else cur.append(c)
      } else c match {
        case '"' => inQ = true; quoted = true
        case ',' => endField()
        case '\n' => endRecord()
        case '\r' => // swallow; \n (or end) closes the record
        case ch => cur.append(ch)
      }
      i += 1
    }
    if (cur.nonEmpty || fields.nonEmpty) endRecord()
    recs.toSeq
  }

  private def col(a: Array[String], i: Int): String = if (i < a.length) a(i) else null
  private def col(s: Seq[String], i: Int): String = if (i < s.length) s(i) else null

  /** file(...,'xml') / xmlparser: for each <rootName> element, {childTag: text} as json. */
  private def readXmlRecords(dir: String, name: String, rootName: String): Array[String] = {
    try {
      val f = new java.io.ByteArrayInputStream(readAll(dir, name).getBytes(StandardCharsets.UTF_8))
      val doc = javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(f)
      val els = doc.getElementsByTagName(rootName)
      (0 until els.getLength).map { i =>
        val rec = mutable.LinkedHashMap[String, String]()
        val kids = els.item(i).getChildNodes
        (0 until kids.getLength).foreach { j =>
          val ch = kids.item(j)
          if (ch.getNodeType == org.w3c.dom.Node.ELEMENT_NODE) rec(ch.getNodeName) = ch.getTextContent
        }
        mapper.writeValueAsString(rec.asJava)
      }.toArray
    } catch { case _: Throwable => Array.empty[String] }
  }

  // U34 strsplitv: split a string into whitespace tokens (Python str.split()).
  def strsplitv(s: String): Array[String] =
    if (s == null) Array.empty else s.split("\\s+").filter(_.nonEmpty)

  // U36/U37 kmeans: 1-D Lloyd clustering of `vals` into k clusters, returning
  // (clusterid, id, points) per input point. Ported from the sklearn KMeans in
  // kmeans_iterative.py / kmeans_recursive.py — same shape, but init differs
  // (deterministic quantile seeds vs sklearn k-means++ random init), so exact
  // cluster labels won't match the Python. Faithful for the GC/JIT workload
  // (the clustering compute is the point), not for reproducing exact labels.
  // `packed` elements are "id\tvalue" (one collect_list, so id/value stay aligned).
  def kmeansCluster(packed: Seq[String], k: Int, maxIter: Int): Array[KCluster] = {
    val pts = packed.flatMap { s =>              // dropna + parse
      if (s == null) None else s.split("\t", 2) match {
        case Array(id, v) => try Some((id, v.toDouble)) catch { case _: Throwable => None }
        case _ => None
      }
    }
    if (pts.isEmpty) return Array.empty
    val data = pts.map(_._2).toArray
    val kk = math.min(k, data.distinct.length)
    if (kk <= 1) return pts.map { case (id, v) => KCluster(0, id, v) }.toArray
    val sorted = data.sorted
    val centroids = Array.tabulate(kk)(i => sorted(math.round(i.toDouble * (sorted.length - 1) / (kk - 1)).toInt))
    val labels = new Array[Int](data.length)
    var iter = 0; var changed = true
    while (iter < maxIter && changed) {
      changed = false
      var i = 0
      while (i < data.length) {
        var best = 0; var bd = math.abs(data(i) - centroids(0)); var c = 1
        while (c < kk) { val d = math.abs(data(i) - centroids(c)); if (d < bd) { bd = d; best = c }; c += 1 }
        if (labels(i) != best) { labels(i) = best; changed = true }
        i += 1
      }
      val sums = new Array[Double](kk); val cnts = new Array[Int](kk)
      i = 0
      while (i < data.length) { sums(labels(i)) += data(i); cnts(labels(i)) += 1; i += 1 }
      var c = 0
      while (c < kk) { if (cnts(c) > 0) centroids(c) = sums(c) / cnts(c); c += 1 }
      iter += 1
    }
    pts.indices.map(i => KCluster(labels(i), pts(i)._1, data(i))).toArray
  }

  def registerAll(spark: SparkSession, filesDir: String): Unit = {
    val u = spark.udf
    u.register("addnoise", addnoise _)
    u.register("clean", clean _)
    u.register("cleandate", cleandate _)
    u.register("converttoeuro", converttoeuro _)
    u.register("extractclass", extractclass _)
    u.register("extractcode", extractcode _)
    u.register("extractday", extractday _)
    u.register("extractfunder", extractfunder _)
    u.register("extractid", extractid _)
    u.register("extractmonth", extractmonth _)
    u.register("extractprojectid", extractprojectid _)
    u.register("extractyear", extractyear _)
    u.register("extractfromdate", (a: String) => extractfromdate(a))
    u.register("filterstopwords", filterstopwords _)
    u.register("frequentterms", frequentterms _)
    u.register("jaccard", jaccard _)
    u.register("jpack", jpack _)
    u.register("jsoncount", jsoncount _)
    u.register("jsonparse_q14", jsonparse _)
    u.register("jsonparse_pair", (j: String, k1: String, k2: String) => jsonparsePair(j, k1, k2))
    u.register("extractkeys", (j: String, k1: String, k2: String) => extractkeys(j, k1, k2))
    u.register("combinations", (s: String, n: Int) => combinations(s, n))
    u.register("jsort", jsort _)
    u.register("jsortvalues", jsortvalues _)
    u.register("keywords", keywords _)
    u.register("log_10", log_10 _)
    u.register("lowerize", lowerize _)
    u.register("removeshortterms", removeshortterms _)
    u.register("stem", stem _)
    // extractmonth_java / extractday_scala: the exec.py Java/Scala UDFs — same
    // date logic, registered here so q3 needs no external jar.
    u.register("extractmonth_java", extractmonth _)
    u.register("extractday_scala", extractday _)

    // File readers (ex-UDTF `file`/`xmlparser`); filesDir captured in the closure.
    u.register("file_text", (name: String) => readLines(filesDir, name).toArray)
    u.register("file_json3", (name: String) =>
      readJsonRecords(filesDir, name).map(r => Cols3(col(r, 0), col(r, 1), col(r, 2))))
    u.register("file_json2", (name: String) =>
      readJsonRecords(filesDir, name).map(r => Cols2(col(r, 0), col(r, 1))))
    u.register("file_csv2", (name: String) =>
      readCsvRecords(filesDir, name).map(r => Cols2(col(r, 0), col(r, 1))))
    u.register("xml_records", (name: String, root: String) => readXmlRecords(filesDir, name, root))

    // q17 tf-idf term splitter, q10/q11 k-means (per-type clustering via collect_list + explode).
    u.register("strsplitv", strsplitv _)
    u.register("kmeans", (packed: Seq[String], k: Int, maxIter: Int) => kmeansCluster(packed, k, maxIter))

    // Aggregate UDFs (pandas_udf -> typed Aggregator).
    u.register("aggregate_avg", functions.udaf(new AvgAgg))
    u.register("aggregate_median", functions.udaf(new MedianAgg))
    u.register("aggregate_max", functions.udaf(new MaxAgg))
    u.register("aggregate_count", functions.udaf(new CountAgg))

    println("UDF_REGISTERED: udfbench scalar+table(all), udaf=avg,median,max,count, files=" + filesDir)
  }
}

/** aggregate_avg: mean of non-null numeric inputs; empty group -> null. */
class AvgAgg extends Aggregator[java.lang.Double, (Double, Long), java.lang.Double] {
  def zero: (Double, Long) = (0.0, 0L)
  def reduce(b: (Double, Long), v: java.lang.Double): (Double, Long) =
    if (v == null) b else (b._1 + v, b._2 + 1)
  def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = (a._1 + b._1, a._2 + b._2)
  def finish(b: (Double, Long)): java.lang.Double = if (b._2 == 0) null else b._1 / b._2
  def bufferEncoder: Encoder[(Double, Long)] = Encoders.tuple(Encoders.scalaDouble, Encoders.scalaLong)
  def outputEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE
}

/** aggregate_median: pandas median (avg of two middles when even); empty -> null.
 *  ponytail: exact median is holistic — it materialises the whole group. The kryo
 *  buffer means the entire group's doubles are serialized at each shuffle boundary
 *  and on spill (O(n) per key); for a global/large-group median (e.g. q4 has no
 *  GROUP BY) that is the whole column in one buffer. Upgrade path if this ever
 *  dominates: swap to an approximate quantile (t-digest) — but that changes the
 *  pandas-exact semantics, so left exact by default. */
case class MedianBuf(v: mutable.ArrayBuffer[Double])
class MedianAgg extends Aggregator[java.lang.Double, MedianBuf, java.lang.Double] {
  def zero: MedianBuf = MedianBuf(mutable.ArrayBuffer.empty[Double])
  def reduce(b: MedianBuf, v: java.lang.Double): MedianBuf = { if (v != null) b.v += v; b }
  def merge(a: MedianBuf, b: MedianBuf): MedianBuf = { a.v ++= b.v; a }
  def finish(b: MedianBuf): java.lang.Double = {
    if (b.v.isEmpty) return null
    val s = b.v.sorted; val n = s.length
    if (n % 2 == 1) s(n / 2) else (s(n / 2 - 1) + s(n / 2)) / 2.0
  }
  def bufferEncoder: Encoder[MedianBuf] = Encoders.kryo(classOf[MedianBuf])
  def outputEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE
}

/** aggregate_max: max of non-null strings. */
class MaxAgg extends Aggregator[String, String, String] {
  def zero: String = null
  def reduce(b: String, v: String): String = if (v == null) b else if (b == null || v > b) v else b
  def merge(a: String, b: String): String = reduce(a, b)
  def finish(b: String): String = b
  def bufferEncoder: Encoder[String] = Encoders.STRING
  def outputEncoder: Encoder[String] = Encoders.STRING
}

/** aggregate_count: count of non-null inputs. */
class CountAgg extends Aggregator[String, Long, java.lang.Long] {
  def zero: Long = 0L
  def reduce(b: Long, v: String): Long = if (v == null) b else b + 1
  def merge(a: Long, b: Long): Long = a + b
  def finish(b: Long): java.lang.Long = b
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  def outputEncoder: Encoder[java.lang.Long] = Encoders.LONG
}
