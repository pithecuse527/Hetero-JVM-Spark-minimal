# TPC-H heap-array amplifier workload

This workload compares the cost of transient on-heap `double[]` allocations
inside a real Spark scalar UDF on the existing HotSpot/G1 and Semeru/OpenJ9
Standalone pools. The same thin jar and the same SQL are submitted to both
masters.

## Confirmed cluster and data layout

The current `spark` namespace has two persistent Standalone pools:

| Pool | Master | Node | Executors | JVM |
|---|---|---|---:|---|
| G1 | `spark-standalone-master-g1` | `worker1` | 3 × 4 core / 8 GiB | HotSpot/OpenJDK + G1 |
| OpenJ9 | `spark-standalone-master-mix` | `worker2` | 3 × 4 core / 8 GiB | Semeru/OpenJ9 + Balanced |

Workers mount the node-local host path `/mnt/bench` at `/mnt/bench` read-only.
The live worker inspection confirmed:

```text
/mnt/bench/tpch-scale-200/{customer,lineitem,nation,orders,part,partsupp,region,supplier}
```

The tables are Parquet directories. The observed `tpch-scale-200` directory is
about 63 GiB on the inspected worker. The runner therefore uses
`file:///mnt/bench/tpch-scale-200` when `--data-base file:///mnt/bench` and
`--scale 200` are supplied.

The manifest also shows a Kubernetes container memory limit below the Spark
worker's advertised 8 GiB on some executor-worker entries. This is an existing
cluster configuration detail, not changed by this workload; verify the live
pod cgroup limit before interpreting an 8 GiB heap run.

```bash
kubectl get pod -n spark <worker-pod> -o jsonpath='{.spec.containers[?(@.name=="spark-worker")].resources}'
kubectl exec -n spark <worker-pod> -- sh -c 'cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes'
```

Do not assume another scale is present. Verify the actual mount and table
format on a live executor worker before changing `--scale`:

```bash
kubectl get pods -n spark -l research-role=spark-standalone-worker -o wide
kubectl exec -n spark <worker-pod> -- find /mnt/bench -maxdepth 2 -type d -name 'tpch-scale-*' -print
kubectl exec -n spark <worker-pod> -- find /mnt/bench/tpch-scale-200/lineitem -maxdepth 1 -type f -name '*.parquet' | head
kubectl exec -n spark <worker-pod> -- du -sh /mnt/bench/tpch-scale-200
```

## Workload shape

The query joins `lineitem` to `orders`, filters `l_shipdate`, and groups by a
parameterized `pmod(o_orderkey, groupCount)` bucket. Only after the join and
GROUP BY does `heap_array_amplify(revenue, group_key)` run once per grouped row.
The UDF allocates one `double[]`, fills and reduces it to one scalar, and the
outer `sum` consumes that scalar. The array is never returned to Spark output.

The UDF is a Spark Scala UDF rather than a native SQL expression because the
Scala UDF is a true opaque JVM call boundary that whole-stage codegen cannot
inline through; the allocation remains ordinary on-heap JVM array allocation
on both JVMs.

## Parameters

Pass these as repeated `--conf` options:

| Conf | Default | Meaning |
|---|---:|---|
| `spark.gcaware.arrayN` | 1024 | doubles allocated per UDF call |
| `spark.gcaware.groupCount` | 100000 | number of order-key hash buckets |
| `spark.gcaware.dateStart` | 1995-01-01 | inclusive ship-date filter |
| `spark.gcaware.dateEnd` | 1996-01-01 | exclusive ship-date filter |

Recommended allocation sweep:

```text
1024       ~= 8 KiB
100000     ~= 800 KiB
1048576    ~= 8 MiB
```

At an 8 GiB heap, the 1,048,576-double array is larger than the usual G1
humongous-object threshold when the G1 region size is 4 MiB; record the actual
HotSpot region setting in each experiment arm rather than assuming it.

## Approximately one-minute tuning

There has not been a measured calibration run for this new workload, so the
following is a reproducible starting point, not a claimed timing result:

```text
scale=200, dateStart=1995-01-01, dateEnd=1996-01-01,
groupCount=100000, arrayN=1048576
```

Tune one knob at a time while holding the JVM arm and all Spark resources fixed:

1. Change `dateStart/dateEnd` to change the join/input work.
2. Change `groupCount` to change the number of post-aggregation UDF calls.
3. Change `arrayN` only for the allocation-size sweep.

Use the emitted `AMPLIFIER_INFO` and final query result to record the intended
allocation volume (`groups × arrayN × 8` bytes). Use the final physical plan and
executor logs to confirm that the UDF is below the aggregate and that all three
executor JVMs participated. A one-minute target must be calibrated on the live
cluster; do not infer it from the 63 GiB directory size.

## Build

```bash
cd research-related/sql-workloads
./build.sh
```

The output is `target/sql-workloads-1.0.jar`. Copy it to the shared Standalone
jar location expected by the submit path:

```bash
cp target/sql-workloads-1.0.jar /path/to/spark-logs/jars/sql-workloads-1.0.jar
```

On the cluster this must be visible inside workers as:
`/var/spark-logs/jars/sql-workloads-1.0.jar`.

## Submit example

The following is the minimal G1 example. Replace only `--standalone-pool` and
`--gc` for the OpenJ9 arm; fill GC/SCC/JITServer controls in the experiment arm
as required, not in this workload:

```bash
cd research-related/scripts/spark_submit
./run-screening.sh \
  --cluster-manager standalone \
  --standalone-pool g1 \
  --bench heapamplifier \
  --query heap-amplifier \
  --scale 200 \
  --data-base file:///mnt/bench \
  --gc g1 \
  --heap 6g --overhead 1g --cores 4 --instances 3 \
  --driver-mem 4g --driver-overhead 1g --driver-cores 2 \
  --aqe false --broadcast off --jfr false --jmx false \
  --conf spark.memory.offHeap.enabled=false \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.gcaware.arrayN=1048576 \
  --conf spark.gcaware.groupCount=100000 \
  --conf spark.gcaware.dateStart=1995-01-01 \
  --conf spark.gcaware.dateEnd=1996-01-01 \
  --tag heap-array-n1m-g1-r1
```

The existing Standalone resource-placement labels may require the repository's
standard `--conf spark.driver.resource.heterojvm_driver.amount=1` and
`--conf spark.executor.resource.heterojvm_executor.amount=1` controls; preserve
the same placement controls for both arms when using them.

The example uses a 6 GiB heap plus 1 GiB overhead because the live worker
cgroup limit was 7 GiB even though the Spark worker advertises 8 GiB. If the
worker pod limit is corrected to 8 GiB, raise the heap only after verifying the
limit and keeping overhead fixed.

## Source files

- `src/main/scala/com/research/gcaware/HeapArrayAmplifierRunner.scala`
- `src/main/resources/heap-amplifier.sql`
- `scripts/spark_submit/run-screening.sh` (`--bench heapamplifier` dispatch)
