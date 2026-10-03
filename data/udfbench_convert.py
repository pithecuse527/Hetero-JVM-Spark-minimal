#!/usr/bin/env python3
"""UDFBench CSV->parquet, pandas (= the reference: pd.read_csv(header=None)).

Whole-table reads (memory is ample on a dedicated worker1 pod). Small tables
run in parallel; the one giant table (views_stats) runs alone to bound peak RAM.
Output layout matches the shipped tiny: <out>/<scale>/parquet/<table>/<table>.parquet
"""
import os, sys, time
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd

CSV_ROOT = "/mnt/bench/udfbench/_work/csvs"
OUT_ROOT = "/mnt/bench/udfbench"
BIG = "views_stats"          # run alone
PARALLEL = 6                 # concurrent small-table workers

def convert_one(scale, table):
    src = f"{CSV_ROOT}/{scale}/{table}.csv"
    dst_dir = f"{OUT_ROOT}/{scale}/parquet/{table}"
    os.makedirs(dst_dir, exist_ok=True)
    dst = f"{dst_dir}/{table}.parquet"
    t = time.time()
    df = pd.read_csv(src, header=None)
    rows = len(df)
    df.to_parquet(dst)
    del df
    return table, rows, time.time() - t

def convert_scale(scale):
    tables = [os.path.splitext(f)[0] for f in os.listdir(f"{CSV_ROOT}/{scale}")
              if f.endswith(".csv")]
    small = [t for t in tables if t != BIG]
    total = 0
    print(f"=== {scale}: {len(tables)} tables ===", flush=True)
    with ProcessPoolExecutor(max_workers=PARALLEL) as ex:
        futs = {ex.submit(convert_one, scale, t): t for t in small}
        for fu in as_completed(futs):
            tbl, rows, dt = fu.result()
            total += rows
            print(f"  {scale}/{tbl}: {rows} rows ({dt:.1f}s)", flush=True)
    if BIG in tables:                       # big table alone
        tbl, rows, dt = convert_one(scale, BIG)
        total += rows
        print(f"  {scale}/{tbl}: {rows} rows ({dt:.1f}s)", flush=True)
    print(f"=== {scale} TOTAL rows: {total} ===", flush=True)
    return total

if __name__ == "__main__":
    for scale in sys.argv[1:] or ["small", "medium", "large"]:
        convert_scale(scale)
