#!/usr/bin/env python3
"""UDFBench CSV -> Parquet for ONE scale. Reads every *.csv in --input and
writes file://.../parquet/<table>/ with the Phase-1-validated options
(header=false, multiLine=true, quote='"', escape='"'). Writes a _rowcounts.txt
next to the parquet dir. Runs as a worker1-pinned k8s Spark job (see
udfbench-csv2parquet.sh) - NOT on the measured g1 standalone pool.
"""
import sys, argparse
from pyspark.sql import SparkSession

# quote/escape are constant for ALL tables (backslash-escape fix from Phase 1).
# multiLine is per-table: TRUE only for tables with (possibly) embedded newlines;
# FALSE elsewhere so the CSV is splittable and uses every local core - the speed
# lever for the giant views_stats. TRUE set = proven-multiline (artifacts,
# projects) + text-heavy (abstract/authorlist) kept safe against newlines that a
# tiny sample may not show. Verified on tiny: every table's row count matches the
# reference under this policy.
BASE_OPTS = dict(header="false", quote='"', escape='"')
MULTILINE_TRUE = {"artifacts", "projects", "artifact_abstracts", "artifact_authorlists"}

def opts_for(table):
    return dict(BASE_OPTS, multiLine="true" if table in MULTILINE_TRUE else "false")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="dir of <table>.csv (file:// or path)")
    ap.add_argument("--output", required=True, help="parquet output dir (file:// or path)")
    ap.add_argument("--scale", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"udfbench-csv2parquet-{args.scale}").getOrCreate()
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI.create(args.input), hconf)
    Path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path

    stats = fs.listStatus(Path(args.input))
    csvs = sorted(s.getPath().getName() for s in stats
                  if s.getPath().getName().endswith(".csv"))
    if not csvs:
        print(f"FATAL: no .csv under {args.input}", file=sys.stderr); sys.exit(1)

    counts = []
    for name in csvs:
        table = name[:-4]
        src = args.input.rstrip("/") + "/" + name
        dst = args.output.rstrip("/") + "/parquet/" + table
        o = opts_for(table)
        print(f"[{args.scale}] {table}: reading {src} (multiLine={o['multiLine']})")
        (spark.read.options(**o).csv(src)
              .write.mode("overwrite").parquet(dst))
        n = spark.read.parquet(dst).count()          # parquet count = metadata-fast
        counts.append((table, n))
        print(f"[{args.scale}] {table}: rows={n} -> {dst}")

    total = sum(n for _, n in counts)
    report = "\n".join(f"{t}\t{n}" for t, n in counts) + f"\nTOTAL\t{total}\n"
    print(f"\n===== {args.scale} ROW COUNTS =====\n{report}")

    # persist counts next to the parquet for later inspection via the helper pod
    rc = args.output.rstrip("/") + "/_rowcounts.txt"
    out = fs.create(Path(rc), True)
    out.write(bytearray(report, "utf-8")); out.close()
    print(f"[{args.scale}] wrote {rc}")
    spark.stop()

if __name__ == "__main__":
    main()
