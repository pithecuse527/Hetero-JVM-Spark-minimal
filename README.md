# Hetero-JVM-Spark minimal artifact

This directory is the minimal source-only artifact for the Spark JVM study.

*Paper: Understanding the Break-Even of OpenJ9 Shared Class Caching on Apache Spark*

It contains the workloads used by the paper, the active single-application submit
path, data-preparation helpers, JVM image recipes, and experiment definitions.

## Contents

- `workloads/`: Scala runners and SQL resources. Runner objects live under
  `com.research.gcaware`:
  - `TpchQueryRunner`, `TpcdsQueryRunner` (`ScreeningJob.scala`) --- the standard
    TPC-H / TPC-DS SQL queries, no UDF. TPC-H is the paper's headline workload.
  - `UdfbenchQueryRunner` --- UDFBench, real scalar / aggregate / table UDFs
    (`Porter2.scala` is the Porter2 stemmer used by the tf-idf query).
  - `HeapArrayAmplifierRunner`, `TpchArrayAmplifierRunner` --- the synthetic
    heap-array UDF microbenchmark (`heap-amplifier.sql`; see
    `HEAP-ARRAY-AMPLIFIER.md`).
  - `JitKernelRunner` --- the synthetic JIT-tax kernel.
  - SQL resources: `tpch/q1..q22.sql`, `udfbench/q1..q19.sql` (+ `stopwords.txt`),
    `udf/tpch/q{1,3}-udf.sql` (UDF-augmented TPC-H), `heap-amplifier.sql`.
- `execution/`: `run-screening.sh` (single-application submit + arm selection),
  `parse-run.sh` (per-run internal-state card), `clear-runtime-caches.sh`, and
  `experiments/template-standalone.json` (a Standalone run definition without
  measured values).
- `data/`: TPC-H generation and UDFBench download/conversion helpers. Generated
  datasets are not redistributed.
- `environment/`: the HotSpot and Semeru/OpenJ9 image recipes.
- `configs/`: reserved for local, deployment-specific overrides; empty in this
  bundle (the run definition template ships under `execution/experiments/`).

## Workloads and scale factors

- **TPC-H** (`--bench tpch`): 22 queries, native scale factors. The paper sweeps
  **SF10–SF90**; the break-even map is reported at SF40–SF90 (4 GB heap).
- **UDFBench** (`--bench udfbench --scale small|medium|large`): **19 runnable
  queries** (`q1`–`q19`). `q20`/`q21` are DML (UPDATE/INSERT) and are excluded ---
  they do not run against Spark temporary views. Data ships at three named sizes
  (small ≈ 0.6 GB, medium ≈ 2.3 GB, large ≈ 3.0 GB). Queries span scalar,
  aggregate (UDAF), and table (UDTF) UDFs.
- **Microbenchmarks**: the heap-array amplifier and the JIT kernel
  (`--bench jitkernel`) locate the mechanism behind the break-even (fixed
  warmup saving vs. steady-state compute); they are supporting, not headline.

## GC / JVM treatments (arms)

Runs use a **homogeneous two-pool** Standalone layout: a HotSpot/G1 pool and an
OpenJ9/Semeru pool with identical Spark resources and data. Select the pool with
`--standalone-pool {g1|openj9}` and the collector/treatment with `--gc`:

| Arm | Flags | Role |
|---|---|---|
| HotSpot + G1 | `--standalone-pool g1 --gc g1` | baseline |
| OpenJ9 gencon + SCC | `--standalone-pool openj9 --gc gencon --openj9-scc true` | treatment (SCC/AOT warmup) |
| OpenJ9 gencon, SCC off | `--standalone-pool openj9 --gc gencon --openj9-scc false` | isolates the SCC effect |
| OpenJ9 balanced | `--standalone-pool openj9 --gc balanced` | region GC / arraylet |

SCC controls: `--openj9-scc-name` (share or isolate the persistent cache across
runs), `--openj9-sccmx` (soft cap). The OpenJ9 SCC uses
`-Xshareclasses:...,cacheDir=/scc,persistent`; mount that path writably and
identically on every OpenJ9 node. Keep query, data, resources, AQE, broadcast
policy, and cache state fixed within a comparison, and use a **fresh application
JVM per submit**.

## Requirements

- JDK 21
- Maven 3.9+
- Spark 4.1.2 with **Scala 2.13** (runtime is Scala 2.13.17)
- Bash, Kubernetes CLI, and a Spark Standalone or Kubernetes deployment for runs
- Linux/amd64 Docker builder when rebuilding the published JVM images

## Build the workload JAR

```bash
cd workloads
./build.sh
```

The build uses only resources contained in this artifact and produces
`workloads/target/sql-workloads-1.0.jar`. Record the printed SHA-256 with every
run. The parent Spark source checkout is not required.

## Prepare input data

TPC-H data are generated rather than redistributed:

```bash
data/datagen-tpch-spark-experiment.sh --scale 10 --dry-run
```

For UDFBench, `data/udfbench-data-fetch.sh` fetches the upstream Zenodo dataset;
`data/udfbench-csv2parquet.sh` converts it to the named parquet scales the Scala
runner reads (`<data-base>/udfbench/<scale>/parquet`). Review and set the
Kubernetes namespace, node names, image, storage path, and capacity gates before
a real run.

## Dry-run one application

The checked-in endpoints are non-routable placeholders. Supply the endpoints
for your deployment through environment variables.

```bash
cd execution
export SPARK_HOME=/path/to/spark-4.1.2-bin-hadoop3
export STANDALONE_G1_MASTER=spark://g1-master.example.org:6066

DRY_RUN=1 ./run-screening.sh \
  --cluster-manager standalone --standalone-pool g1 \
  --bench tpch --query q1 --scale 10 --gc g1 \
  --heap 8g  --cores 4 --instances 4 \
  --driver-mem 4g --driver-cores 2 \
  --aqe true --broadcast default --jfr false --jmx false \
  --tag artifact-smoke
```

For OpenJ9, set `STANDALONE_MIX_MASTER`, use `--standalone-pool openj9 --gc gencon`,
and add `--openj9-scc true --openj9-scc-name <cache>` for the SCC treatment (or
`--openj9-scc false` for the control).

## Verbose JIT / GC capture (optional)

`--warmup-verbose true` emits a compile trace so AOT/SCC loads vs. fresh JIT
compiles can be counted (OpenJ9 `-Xjit:verbose={compilePerformance}` vlog; the
HotSpot side uses `-Xlog:jit+compilation` + `-XX:+CITime`, injectable via
`--extra-exec-opts`). Verbose logging perturbs wall-clock, so keep it off for
timing runs and enable it only on a few dedicated runs. Traces are written to the
executor pod log mount (not the local run folder); collect them from there.

`parse-run.sh <run-folder>` reads a persisted run folder (`stdout`, `stderr`,
`provenance.txt`, `scc-before.txt`/`scc-after.txt`, `gc/*.log`) into a compact
internal-state card so GC/SCC verdicts are read from the JVM's own logs.

## Reproduction notes

- **Build with Scala 2.13.** A Scala 2.12 jar fails at run time with
  `NoSuchMethodError: scala.Predef$.refArrayOps`.
- **Standalone driver memory.** `--driver-mem` must not exceed the memory the
  driver's Standalone worker advertises to the master, or the driver hangs in
  `SUBMITTED` and never launches.
- **SCC warm-up.** Discard the first (cold-populate) run per cache, then measure
  warm runs. The cache is framework-dominated and shared across queries, so one
  cold-populate per cache/scale is sufficient.
- **Fair comparison.** Compare `(scc − g1)` within a fixed scale/resource point;
  only absolute times depend on the heap/core choice, not the direction.
- **Provenance.** Record the workload-jar SHA-256 and the exact (immutable) image
  digests with every run; mutable image tags alone are insufficient.

## Output boundary

The scripts may create datasets, JARs, run directories, or logs when invoked,
but none of those generated outputs are distributed in this directory. Users
should direct experimental output to a separate location.

## Release checklist

1. Replace mutable image tags with the exact image digests used for the paper.
2. Run a credential and infrastructure-identifier scan.
3. Select a license for the original artifact code and retain the third-party
   notices (`THIRD_PARTY.md`, `licenses/`).
