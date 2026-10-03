#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# GC Screening Experiment Script — one Spark run on the k3s `spark` namespace.
#
# Named flags (order-independent; every knob is explicit — no magic presets):
#   --query <q>        REQUIRED. e.g. q64, q38, q9
#   --cluster-manager <m> default kubernetes. kubernetes | standalone
#   --master <url>     default k8s current context, or pool-specific spark://... for standalone
#   --standalone-pool <p> default auto. g1 | openj9 | auto
#                    auto: HotSpot g1 -> g1 pool; OpenJ9 balanced -> openj9 pool.
#   --standalone-local-tunnel <bool> default false. Use local REST tunnel for Standalone submit.
#   --standalone-g1-local-rest-address <a> default 127.0.0.1. Local loopback address for the G1 tunnel.
#   --standalone-mix-local-rest-address <a> default ::1. Local loopback address for the Mix tunnel.
#   --standalone-collect <m> default kubectl. kubectl | ssh. How Standalone run evidence is
#                    collected: kubectl exec into worker pods, or ssh to bare-metal nodes
#                    (APP_ID from the master UI /json/, driver stdout from the worker work
#                    dir, GC logs and SCC printStats from every node's /logs and /scc).
#   --standalone-ssh-driver-node <h>    ssh mode: driver-worker host ("local" = this host).
#   --standalone-ssh-executor-nodes <l> ssh mode: executor-worker hosts, comma separated.
#   --standalone-work-dir <d> ssh mode: worker work dir, default /opt/heterojvm/<pool>/work.
#   --gc <gc>          default g1.  HotSpot: g1 | zgc(=generational) | shen
#                                    OpenJ9: gencon | balanced | optthruput | optavgpause
#   --bench <b>        default tpcds.  tpcds | tpch | jitkernel | udfbench
#   --scale <n>        default 200
#   --aqe <bool>       default true.  true | false  (false for GC-mechanism isolation)
#   --heap <size>      default 2g (3g for Standalone). executor heap (spark.executor.memory)
#   --cores <n>        default 2      spark.executor.cores
#   --instances <n>    default 2      spark.executor.instances
#   --driver-mem <sz>  default 4g
#   --driver-cores <n> default 2      spark.driver.cores
#   --overhead <sz>    default Spark default (1g for Standalone)  spark.executor.memoryOverhead
#   --driver-overhead <sz> default Spark default (1g for Standalone)  spark.driver.memoryOverhead
#   --executor-cpu-request <n> default <cores> Kubernetes executor CPU request
#   --executor-cpu-limit <n>   default <executor-cpu-request> Kubernetes executor CPU limit
#   --driver-cpu-request <n>   default <driver-cores> Kubernetes driver CPU request
#   --driver-cpu-limit <n>     default <driver-cpu-request> Kubernetes driver CPU limit
#   --broadcast <v>    default off.   "off" (=-1, force SMJ) | a size like 128MB
#   --region <sz>      default "".    G1 only: -XX:G1HeapRegionSize (e.g. 8m)
#   --node <name>      default "".    Pin driver + executors to one Kubernetes node hostname.
#   --data-base <uri>  default file:///mnt/bench (LOCAL disk; pass s3a://spark-obj-storage for S3)
#   --jfr <bool>       default true.  Enable JFR for HotSpot driver + executors.
#   --jmx <bool>       default true.  Enable Prometheus JMX exporter on driver + executors.
#   --jitserver <bool> default false. Enable OpenJ9 JITServer client mode.
#   --jitserver-address <host> default auto from --node for worker1/worker2.
#   --jitserver-port <n> default 38400 (OpenJ9 JITServer default; matches the
#                    jitserver-worker1/2 Services' exposed port).
#   --jitserver-aot-cache <bool> default false. Enable OpenJ9 JITServer AOT cache on clients.
#   --jitserver-aot-cache-name <s> default spark-<benchmark>-<query>-<gc>.
#   --extra-exec-opts <s>  default "".  Extra executor JVM opts (e.g. -XX:NativeMemoryTracking=summary)
#   --openj9-scc <bool>    default true. Pure-OpenJ9 arms: include -Xshareclasses SCC (cacheDir=/scc,persistent).
#   --openj9-scc-name <s>  default sparkudf. Persistent SCC cache name (share/isolate the cache across runs).
#   --openj9-sccmx <sz>    default 300m. -Xscmx soft cap for the pure-OpenJ9 SCC.
#   --warmup-verbose <bool> default false. OpenJ9 only: emit -Xjit:verbose compile
#                    trace (vlog) so AOT/SCC loads vs fresh JIT compiles can be counted.
#                    Default off keeps wall-clock timing runs clean; turn on for the
#                    dedicated AOT-evidence run. HotSpot: no app AOT, flag ignored.
#   Cluster B always uses Semeru/OpenJ9 Balanced with the persistent /scc cache.
#   --conf <k=v>       extra Spark conf; repeatable.
#   --tag <s>          default "".    Suffix appended to RUN_ID (disambiguate reruns)
#   --arm <s>          default <gc>.  Arm label written to provenance.txt as arm=
#                    (e.g. g1, scc, bare, class); the GC policy stays in gc_policy=.
#   SPARK_SUBMIT_MASTER (env) override submit master URL.
#   STANDALONE_LOCAL_TUNNEL (env) true|false, default false.
#   STANDALONE_G1_LOCAL_REST_ADDRESS (env) default 127.0.0.1; STANDALONE_MIX_LOCAL_REST_ADDRESS (env) default ::1.
#   STANDALONE_UI_REVERSE_PROXY_URL (env) override pool-specific UI reverse proxy URL.
#   STANDALONE_COLLECT, STANDALONE_SSH_DRIVER_NODE, STANDALONE_SSH_EXECUTOR_NODES,
#   STANDALONE_WORK_DIR (env) same as the flags above.
#   STANDALONE_POOL_ROOT (env) default /opt/heterojvm; STANDALONE_SSH_USER default root;
#   STANDALONE_SSH_OPTS default "-o BatchMode=yes -o ConnectTimeout=10".
#   STANDALONE_MASTER_JSON_URL (env) default <UI reverse proxy URL>/json/ (ssh APP_ID lookup).
#   SEMERU_JAVA (env) default /opt/java/semeru/bin/java (ssh mode SCC printStats).
#   --warmup-verbose-jit <list> (env WARMUP_VERBOSE_JIT) default compilePerformance. OpenJ9
#                    -Xjit:verbose={<list>} used by --warmup-verbose true, e.g.
#                    JITServer,compilePerformance,compileEnd for the JITServer cause tests.
#   --jitserver-cpu-unit <u> (env JITSERVER_CPU_UNIT) ssh mode, default off: read the
#                    systemd unit's cgroup cpu.stat/memory.events on every ssh node before
#                    submit and after terminal; writes jitserver_cpu_s_* to provenance.txt.
#   STANDALONE_JITSERVER_ADDRESS (env) standalone only: JITServer address used when
#                    --jitserver true is given without --jitserver-address.
#   DRY_RUN=1 (env)    print resolved image + JVM opts and exit without submitting
#
# Pod / run name = {bench}-{query}-{scale}-{gc}-{aqe}-{heap}-{cores}[-r<region>][-<node>]  (lowercased).
#
# Examples:
#   ./run-screening.sh --query q38 --gc shen --heap 4g --aqe false
#   ./run-screening.sh --query q9 --bench tpch --gc zgc --heap 2g --broadcast off --aqe false
#   ./run-screening.sh --query q64 --gc g1 --heap 2g --region 8m --aqe false
#   ./run-screening.sh --query q38 --gc g1 --heap 2g --region 1m --node worker1 --aqe false
# =============================================================================

# --- defaults ---
ARTIFACT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_LOG_ROOT="${LOCAL_LOG_ROOT:-$ARTIFACT_ROOT/runs}"
QUERY=""; CLUSTER_MANAGER="${CLUSTER_MANAGER:-}"; SUBMIT_MASTER="${SPARK_SUBMIT_MASTER:-}"
SUBMIT_MASTER_EXPLICIT="false"
[ -n "$SUBMIT_MASTER" ] && SUBMIT_MASTER_EXPLICIT="true"
STANDALONE_POOL="${STANDALONE_POOL:-auto}"
STANDALONE_LOCAL_TUNNEL="${STANDALONE_LOCAL_TUNNEL:-false}"
STANDALONE_LOCAL_TUNNEL_HOSTS_FILE=""
STANDALONE_LOCAL_TUNNEL_HOSTS_FILE_CREATED="false"
STANDALONE_DEFAULT_MASTER="${STANDALONE_MASTER:-}"
# Public REST endpoints. In-cluster Driver workers resolve these names through
# pod-local host aliases to the corresponding Master Service before connecting
# to the rewritten RPC port (7077).
STANDALONE_G1_MASTER="${STANDALONE_G1_MASTER:-spark://g1-master.example.invalid:6066}"
STANDALONE_MIX_MASTER="${STANDALONE_MIX_MASTER:-spark://openj9-master.example.invalid:6066}"
STANDALONE_G1_LOCAL_REST_ADDRESS="${STANDALONE_G1_LOCAL_REST_ADDRESS:-127.0.0.1}"
STANDALONE_MIX_LOCAL_REST_ADDRESS="${STANDALONE_MIX_LOCAL_REST_ADDRESS:-::1}"
STANDALONE_G1_TUNNEL_MASTER="spark://spark-standalone-master-g1.spark.svc.cluster.local:6066"
STANDALONE_MIX_TUNNEL_MASTER="spark://spark-standalone-master-mix.spark.svc.cluster.local:6066"
STANDALONE_LOCAL_TUNNEL_ADDRESS=""
STANDALONE_LOCAL_TUNNEL_HOST=""
STANDALONE_STATUS_POLL_SECONDS="${STANDALONE_STATUS_POLL_SECONDS:-2}"
STANDALONE_STATUS_TIMEOUT_SECONDS="${STANDALONE_STATUS_TIMEOUT_SECONDS:-1800}"
# The poll interval may be fractional (e.g. 0.25) so terminal detection adds little to
# wall time; elapsed time is tracked in integer milliseconds.
case "$STANDALONE_STATUS_POLL_SECONDS" in
  ''|.*|*.|*[!0-9.]*|*.*.*)
    echo "ERROR: STANDALONE_STATUS_POLL_SECONDS must be a positive number, got" \
      "'$STANDALONE_STATUS_POLL_SECONDS'"; exit 2 ;;
esac
STANDALONE_STATUS_POLL_MS="$(awk -v s="$STANDALONE_STATUS_POLL_SECONDS" \
  'BEGIN { printf "%d", s * 1000 }')"
[ "$STANDALONE_STATUS_POLL_MS" -gt 0 ] || {
  echo "ERROR: STANDALONE_STATUS_POLL_SECONDS must be at least 0.001"; exit 2
}
STANDALONE_G1_UI_REVERSE_PROXY_URL="${STANDALONE_G1_UI_REVERSE_PROXY_URL:-http://g1-ui.example.invalid}"
STANDALONE_MIX_UI_REVERSE_PROXY_URL="${STANDALONE_MIX_UI_REVERSE_PROXY_URL:-http://openj9-ui.example.invalid}"
STANDALONE_UI_REVERSE_PROXY_URL="${STANDALONE_UI_REVERSE_PROXY_URL:-}"
STANDALONE_UI_REVERSE_PROXY_URL_EXPLICIT="false"
[ -n "$STANDALONE_UI_REVERSE_PROXY_URL" ] && STANDALONE_UI_REVERSE_PROXY_URL_EXPLICIT="true"
# Standalone evidence collection. kubectl (default) = worker pods on k3s; ssh = bare-metal
# nodes reached as $STANDALONE_SSH_USER@<node>. A node named "local" runs on this host.
STANDALONE_COLLECT="${STANDALONE_COLLECT:-kubectl}"
STANDALONE_SSH_DRIVER_NODE="${STANDALONE_SSH_DRIVER_NODE:-}"
STANDALONE_SSH_EXECUTOR_NODES="${STANDALONE_SSH_EXECUTOR_NODES:-}"
STANDALONE_SSH_NODES=""
JITSERVER_CPU_UNIT="${JITSERVER_CPU_UNIT:-}"
JITSERVER_CPU_BEFORE=""
JITSERVER_CPU_AFTER=""
STANDALONE_SSH_USER="${STANDALONE_SSH_USER:-root}"
STANDALONE_SSH_OPTS="${STANDALONE_SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=10}"
STANDALONE_POOL_ROOT="${STANDALONE_POOL_ROOT:-/opt/heterojvm}"
STANDALONE_WORK_DIR="${STANDALONE_WORK_DIR:-}"
STANDALONE_MASTER_JSON_URL="${STANDALONE_MASTER_JSON_URL:-}"
SEMERU_JAVA="${SEMERU_JAVA:-/opt/java/semeru/bin/java}"
GC="g1"; BENCHMARK="tpcds"; SCALE="200"; AQE_ENABLED="true"
HEAP="2g"; CORES="2"; INSTANCES="2"; DRIVER_MEMORY="4g"; DRIVER_CORES="2"; OVERHEAD=""
HEAP_EXPLICIT="false"; CORES_EXPLICIT="false"; INSTANCES_EXPLICIT="false"
DRIVER_MEMORY_EXPLICIT="false"; DRIVER_CORES_EXPLICIT="false"; OVERHEAD_EXPLICIT="false"
DRIVER_OVERHEAD=""; DRIVER_OVERHEAD_EXPLICIT="false"; EXECUTOR_CPU_REQUEST=""; EXECUTOR_CPU_LIMIT=""
DRIVER_CPU_REQUEST=""; DRIVER_CPU_LIMIT=""
BROADCAST="off"; REGION=""; NODE=""; DATA_BASE="file:///mnt/bench"
JFR_ENABLED="true"; JMX_ENABLED="true"; JITSERVER_ENABLED="false"; JITSERVER_ADDRESS=""; JITSERVER_PORT="38400"
JMX_EXPLICIT="false"
JITSERVER_AOT_CACHE_ENABLED="false"; JITSERVER_AOT_CACHE_NAME=""
EXTRA_EXEC_OPTS=""; MIXED_OPENJ9_EXTRA_OPTS=""; MIXED_OPENJ9_JITSERVER_ENABLED="true"; MIXED_OPENJ9_SCC_ENABLED="true"; MIXED_OPENJ9_SCC_NAME=""; MIXED_OPENJ9_SCCMX="300m"; MIXED_OPENJ9_AOT_ENABLED="true"; OPENJ9_LOCAL_AOT="true"; OPENJ9_SCCMX="300m"; TAG=""
# Pure-OpenJ9 (non-mixed) shared-class cache controls. Default true/sparkudf
# preserves the previous hardcoded behavior; per-arm wrappers override these.
OPENJ9_SCC_ENABLED="true"; OPENJ9_SCC_NAME="sparkudf"
# Extra OpenJ9 JVM opts appended to BOTH driver and executor extraJavaOptions
# (e.g. -Xaot:enableSVMDuringStartup). Do NOT put -Xgcpolicy here: GC comes from
# --gc so it appears exactly once. Pure-OpenJ9 arms only.
OPENJ9_EXTRA_OPTS=""
WARMUP_VERBOSE="false"
WARMUP_VERBOSE_JIT="${WARMUP_VERBOSE_JIT:-compilePerformance}"
ARM_LABEL=""
USER_SPARK_CONF_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --query)        QUERY="${2:?}"; shift ;;
    --cluster-manager) CLUSTER_MANAGER="${2:?}"; shift ;;
    --master)       SUBMIT_MASTER="${2:?}"; SUBMIT_MASTER_EXPLICIT="true"; shift ;;
    --standalone-pool) STANDALONE_POOL="${2:?}"; shift ;;
    --standalone-local-tunnel) STANDALONE_LOCAL_TUNNEL="${2:?}"; shift ;;
    --standalone-g1-local-rest-address) STANDALONE_G1_LOCAL_REST_ADDRESS="${2:?}"; shift ;;
    --standalone-mix-local-rest-address) STANDALONE_MIX_LOCAL_REST_ADDRESS="${2:?}"; shift ;;
    --standalone-ui-url) STANDALONE_UI_REVERSE_PROXY_URL="${2:?}"; STANDALONE_UI_REVERSE_PROXY_URL_EXPLICIT="true"; shift ;;
    --standalone-collect) STANDALONE_COLLECT="${2:?}"; shift ;;
    --standalone-ssh-driver-node) STANDALONE_SSH_DRIVER_NODE="${2:?}"; shift ;;
    --standalone-ssh-executor-nodes) STANDALONE_SSH_EXECUTOR_NODES="${2:?}"; shift ;;
    --standalone-work-dir) STANDALONE_WORK_DIR="${2:?}"; shift ;;
    --jitserver-cpu-unit) JITSERVER_CPU_UNIT="${2:?}"; shift ;;
    --gc)           GC="${2:?}"; shift ;;
    --bench|--benchmark) BENCHMARK="${2:?}"; shift ;;
    --scale)        SCALE="${2:?}"; shift ;;
    --aqe)          AQE_ENABLED="${2:?}"; shift ;;
    --heap)         HEAP="${2:?}"; HEAP_EXPLICIT="true"; shift ;;
    --cores)        CORES="${2:?}"; CORES_EXPLICIT="true"; shift ;;
    --instances)    INSTANCES="${2:?}"; INSTANCES_EXPLICIT="true"; shift ;;
    --driver-mem)   DRIVER_MEMORY="${2:?}"; DRIVER_MEMORY_EXPLICIT="true"; shift ;;
    --driver-cores) DRIVER_CORES="${2:?}"; DRIVER_CORES_EXPLICIT="true"; shift ;;
    --overhead)     OVERHEAD="${2:?}"; OVERHEAD_EXPLICIT="true"; shift ;;
    --driver-overhead) DRIVER_OVERHEAD="${2:?}"; DRIVER_OVERHEAD_EXPLICIT="true"; shift ;;
    --executor-cpu-request) EXECUTOR_CPU_REQUEST="${2:?}"; shift ;;
    --executor-cpu-limit) EXECUTOR_CPU_LIMIT="${2:?}"; shift ;;
    --driver-cpu-request) DRIVER_CPU_REQUEST="${2:?}"; shift ;;
    --driver-cpu-limit) DRIVER_CPU_LIMIT="${2:?}"; shift ;;
    --broadcast)    BROADCAST="${2:?}"; shift ;;
    --region)       REGION="${2:?}"; shift ;;
    --node)         NODE="${2:?}"; shift ;;
    --data-base)    DATA_BASE="${2:?}"; shift ;;
    --jfr)          JFR_ENABLED="${2:?}"; shift ;;
    --jmx)          JMX_ENABLED="${2:?}"; JMX_EXPLICIT="true"; shift ;;
    --jitserver)    JITSERVER_ENABLED="${2:?}"; shift ;;
    --jitserver-address) JITSERVER_ADDRESS="${2:?}"; shift ;;
    --jitserver-port) JITSERVER_PORT="${2:?}"; shift ;;
    --jitserver-aot-cache) JITSERVER_AOT_CACHE_ENABLED="${2:?}"; shift ;;
    --jitserver-aot-cache-name) JITSERVER_AOT_CACHE_NAME="${2:?}"; shift ;;
    --extra-exec-opts) EXTRA_EXEC_OPTS="${2:?}"; shift ;;
    --mixed-openj9-extra-opts) MIXED_OPENJ9_EXTRA_OPTS="${2:?}"; shift ;;
    --mixed-openj9-jitserver) MIXED_OPENJ9_JITSERVER_ENABLED="${2:?}"; shift ;;
    --mixed-openj9-scc) MIXED_OPENJ9_SCC_ENABLED="${2:?}"; shift ;;
    --mixed-openj9-scc-name) MIXED_OPENJ9_SCC_NAME="${2:?}"; shift ;;
    --mixed-openj9-sccmx) MIXED_OPENJ9_SCCMX="${2:?}"; shift ;;
    --mixed-openj9-aot) MIXED_OPENJ9_AOT_ENABLED="${2:?}"; shift ;;
    --openj9-local-aot) OPENJ9_LOCAL_AOT="${2:?}"; shift ;;
    --openj9-sccmx) OPENJ9_SCCMX="${2:?}"; shift ;;
    --openj9-scc)      OPENJ9_SCC_ENABLED="${2:?}"; shift ;;
    --openj9-scc-name) OPENJ9_SCC_NAME="${2:?}"; shift ;;
    --openj9-extra-opts) OPENJ9_EXTRA_OPTS="${2:?}"; shift ;;
    --warmup-verbose) WARMUP_VERBOSE="${2:?}"; shift ;;
    --warmup-verbose-jit) WARMUP_VERBOSE_JIT="${2:?}"; shift ;;
    --conf)         USER_SPARK_CONF_ARGS+=(--conf "${2:?}"); shift ;;
    --tag)          TAG="${2:?}"; shift ;;
    --arm)          ARM_LABEL="${2:?}"; shift ;;
    -h|--help)      sed -n '4,59p' "$0"; exit 0 ;;
    *) echo "ERROR: unknown arg '$1' (see --help)"; exit 2 ;;
  esac
  shift
done
[ -n "$QUERY" ] || { echo "ERROR: --query is required (see --help)"; exit 2; }
case "$AQE_ENABLED" in true|false) ;; *) echo "ERROR: --aqe must be true|false, got '$AQE_ENABLED'"; exit 2 ;; esac
case "$JFR_ENABLED" in true|false) ;; *) echo "ERROR: --jfr must be true|false, got '$JFR_ENABLED'"; exit 2 ;; esac
case "$JMX_ENABLED" in true|false) ;; *) echo "ERROR: --jmx must be true|false, got '$JMX_ENABLED'"; exit 2 ;; esac
case "$STANDALONE_LOCAL_TUNNEL" in true|false) ;; *) echo "ERROR: --standalone-local-tunnel must be true|false, got '$STANDALONE_LOCAL_TUNNEL'"; exit 2 ;; esac
case "$JITSERVER_ENABLED" in true|false) ;; *) echo "ERROR: --jitserver must be true|false, got '$JITSERVER_ENABLED'"; exit 2 ;; esac
case "$JITSERVER_PORT" in ''|*[!0-9]*) echo "ERROR: --jitserver-port must be a positive integer, got '$JITSERVER_PORT'"; exit 2 ;; esac
case "$JITSERVER_AOT_CACHE_ENABLED" in true|false) ;; *) echo "ERROR: --jitserver-aot-cache must be true|false, got '$JITSERVER_AOT_CACHE_ENABLED'"; exit 2 ;; esac
case "$MIXED_OPENJ9_JITSERVER_ENABLED" in true|false) ;; *) echo "ERROR: --mixed-openj9-jitserver must be true|false, got '$MIXED_OPENJ9_JITSERVER_ENABLED'"; exit 2 ;; esac
case "$MIXED_OPENJ9_SCC_ENABLED" in true|false) ;; *) echo "ERROR: --mixed-openj9-scc must be true|false, got '$MIXED_OPENJ9_SCC_ENABLED'"; exit 2 ;; esac
case "$MIXED_OPENJ9_AOT_ENABLED" in true|false) ;; *) echo "ERROR: --mixed-openj9-aot must be true|false, got '$MIXED_OPENJ9_AOT_ENABLED'"; exit 2 ;; esac
case "$OPENJ9_LOCAL_AOT" in true|false) ;; *) echo "ERROR: --openj9-local-aot must be true|false, got '$OPENJ9_LOCAL_AOT'"; exit 2 ;; esac
case "$OPENJ9_SCC_ENABLED" in true|false) ;; *) echo "ERROR: --openj9-scc must be true|false, got '$OPENJ9_SCC_ENABLED'"; exit 2 ;; esac
case "$WARMUP_VERBOSE" in true|false) ;; *) echo "ERROR: --warmup-verbose must be true|false, got '$WARMUP_VERBOSE'"; exit 2 ;; esac
case "$WARMUP_VERBOSE_JIT" in
  ''|*[!A-Za-z,]*)
    echo "ERROR: --warmup-verbose-jit must be a comma list of OpenJ9 verbose names," \
      "got '$WARMUP_VERBOSE_JIT'"; exit 2 ;;
esac
case "$ARM_LABEL" in
  *[!A-Za-z0-9_.-]*)
    echo "ERROR: --arm must use letters/digits/dot/underscore/hyphen, got '$ARM_LABEL'"; exit 2 ;;
esac
case "$OPENJ9_SCC_NAME" in *[!A-Za-z0-9_.-]*) echo "ERROR: --openj9-scc-name must contain only letters/digits/dot/underscore/hyphen, got '$OPENJ9_SCC_NAME'"; exit 2 ;; esac

validate_loopback_address() {
  local address="$1"
  local option_name="$2"
  case "$address" in
    127.0.0.1|::1) ;;
    *) echo "ERROR: $option_name must be 127.0.0.1 or ::1, got '$address'" >&2; exit 2 ;;
  esac
}
validate_loopback_address "$STANDALONE_G1_LOCAL_REST_ADDRESS" "--standalone-g1-local-rest-address"
validate_loopback_address "$STANDALONE_MIX_LOCAL_REST_ADDRESS" "--standalone-mix-local-rest-address"

if [ -z "$CLUSTER_MANAGER" ]; then
  case "$SUBMIT_MASTER" in
    spark://*) CLUSTER_MANAGER="standalone" ;;
    *) CLUSTER_MANAGER="kubernetes" ;;
  esac
fi
case "$CLUSTER_MANAGER" in
  kubernetes|k8s) CLUSTER_MANAGER="kubernetes" ;;
  standalone) ;;
  *) echo "ERROR: --cluster-manager must be kubernetes|standalone, got '$CLUSTER_MANAGER'"; exit 2 ;;
esac
if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  if [ "$JMX_EXPLICIT" = "false" ]; then
    JMX_ENABLED="false"
  elif [ "$JMX_ENABLED" = "true" ]; then
    echo "WARN: standalone --jmx true requires /var/spark-logs/jars/jmx_prometheus_javaagent.jar and /etc/jmx-exporter/jmx-exporter.yaml inside Standalone driver/executor workers." >&2
  fi
else
  if [ -n "$SUBMIT_MASTER" ]; then
    case "$SUBMIT_MASTER" in
      k8s://*) ;;
      *) echo "ERROR: kubernetes mode requires --master k8s://... when --master is supplied, got '$SUBMIT_MASTER'"; exit 2 ;;
    esac
  fi
fi

if [ -z "$EXECUTOR_CPU_REQUEST" ] && [ -z "$EXECUTOR_CPU_LIMIT" ]; then
  EXECUTOR_CPU_REQUEST="$CORES"
  EXECUTOR_CPU_LIMIT="$CORES"
elif [ -z "$EXECUTOR_CPU_REQUEST" ]; then
  EXECUTOR_CPU_REQUEST="$EXECUTOR_CPU_LIMIT"
elif [ -z "$EXECUTOR_CPU_LIMIT" ]; then
  EXECUTOR_CPU_LIMIT="$EXECUTOR_CPU_REQUEST"
fi
if [ -z "$DRIVER_CPU_REQUEST" ] && [ -z "$DRIVER_CPU_LIMIT" ]; then
  DRIVER_CPU_REQUEST="$DRIVER_CORES"
  DRIVER_CPU_LIMIT="$DRIVER_CORES"
elif [ -z "$DRIVER_CPU_REQUEST" ]; then
  DRIVER_CPU_REQUEST="$DRIVER_CPU_LIMIT"
elif [ -z "$DRIVER_CPU_LIMIT" ]; then
  DRIVER_CPU_LIMIT="$DRIVER_CPU_REQUEST"
fi
if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  [ "$EXECUTOR_CPU_REQUEST" = "$EXECUTOR_CPU_LIMIT" ] || {
    echo "ERROR: executor CPU request and limit must match for isolated runs: request=$EXECUTOR_CPU_REQUEST limit=$EXECUTOR_CPU_LIMIT" >&2
    exit 2
  }
  [ "$DRIVER_CPU_REQUEST" = "$DRIVER_CPU_LIMIT" ] || {
    echo "ERROR: driver CPU request and limit must match for isolated runs: request=$DRIVER_CPU_REQUEST limit=$DRIVER_CPU_LIMIT" >&2
    exit 2
  }
fi

if [ -n "$NODE" ]; then
  case "$NODE" in
    *[!A-Za-z0-9_.-]*) echo "ERROR: --node must be a Kubernetes node hostname (letters/digits/dot/underscore/hyphen), got '$NODE'"; exit 2 ;;
  esac
fi

# --- Benchmark -> main class + data location ---
case "$BENCHMARK" in
  tpcds) MAIN_CLASS="com.research.gcaware.TpcdsQueryRunner"; DATA_LOCATION="${DATA_BASE%/}/tpcds-scale-$SCALE" ;;
  tpch)  MAIN_CLASS="com.research.gcaware.TpchQueryRunner";  DATA_LOCATION="${DATA_BASE%/}/tpch-scale-$SCALE" ;;
  jitkernel) MAIN_CLASS="com.research.gcaware.JitKernelRunner"; DATA_LOCATION="unused://jitkernel" ;;
  udfbench)
    case "$SCALE" in
      small|medium|large) ;;
      *) echo "ERROR: --bench udfbench requires --scale small|medium|large, got '$SCALE'"; exit 2 ;;
    esac
    MAIN_CLASS="com.research.gcaware.UdfbenchQueryRunner"
    # UDFBench data is name-scaled node-local parquet; external files default to
    # /mnt/bench/udfbench/files/<scale> inside the jar (no extra wiring needed).
    DATA_LOCATION="${DATA_BASE%/}/udfbench/$SCALE/parquet"
    ;;
  *) echo "ERROR: --bench must be tpcds|tpch|jitkernel|udfbench, got '$BENCHMARK'"; exit 2 ;;
esac

# --- Broadcast threshold: "off" => -1 (force SMJ, GC-isolation case);
#     "default"/"on" => leave Spark default (10MB) by OMITTING the conf (BC="");
#     else an explicit size like 128MB ---
case "$BROADCAST" in
  off|-1)     BC="-1" ;;
  default|on) BC="" ;;
  *)          BC="$BROADCAST" ;;
esac

# --- GC -> JVM family, image, GC flags ---
#   HotSpot collectors (g1/zgc/shen) -> Temurin image, -Xlog GC logging.
#   OpenJ9 policies (gencon/balanced/optthruput/optavgpause) -> Semeru image, -Xverbosegclog.
IMAGE="${SPARK_K8S_IMAGE:-apache/spark:4.1.2}"
JVM_FAMILY="hotspot"
JVM_MIX="none"
MIXED_OPENJ9_POLICY=""
HOTSPOT_GC_LOG_COMMON="gc*,gc+heap=debug,gc+phases=debug,gc+age=trace,gc+ergo=trace,gc+ref=debug,gc+cpu=debug,safepoint=debug"
case "$GC" in
  g1)
    GC_OPTS="-XX:+UseG1GC"
    GC_LOG_SEL="$HOTSPOT_GC_LOG_COMMON,gc+humongous=debug,gc+ihop=debug,gc+remset=debug,gc+region=debug"
    [ -n "$REGION" ] && GC_OPTS="$GC_OPTS -XX:G1HeapRegionSize=$REGION"
    ;;
  zgc)
    GC_OPTS="-XX:+UseZGC -XX:+ZGenerational"
    GC_LOG_SEL="$HOTSPOT_GC_LOG_COMMON"
    ;;
  shen|shenandoah)
    GC="shen"
    GC_OPTS="-XX:+UseShenandoahGC"
    GC_LOG_SEL="$HOTSPOT_GC_LOG_COMMON"
    ;;
  gencon|balanced|optthruput|optavgpause)
    JVM_FAMILY="openj9"
    IMAGE="${OPENJ9_IMAGE:-}"
    GC_OPTS="-Xgcpolicy:$GC"
    # NOTE: --region is HotSpot-only and ignored on OpenJ9 (balanced would use -Xgc:regionSize).
    ;;
  mix-gencon|mixture-gencon|g1-gencon)
    GC="mix-gencon"
    JVM_FAMILY="mixed"
    JVM_MIX="g1-gencon"
    MIXED_OPENJ9_POLICY="gencon"
    IMAGE="${OPENJ9_IMAGE:-}"
    GC_OPTS="-XX:+UseG1GC"
    GC_LOG_SEL="$HOTSPOT_GC_LOG_COMMON,gc+humongous=debug,gc+ihop=debug,gc+remset=debug,gc+region=debug"
    ;;
  mix-balanced|mixture-balanced|g1-balanced)
    GC="mix-balanced"
    JVM_FAMILY="mixed"
    JVM_MIX="g1-balanced"
    MIXED_OPENJ9_POLICY="balanced"
    IMAGE="${OPENJ9_IMAGE:-}"
    GC_OPTS="-XX:+UseG1GC"
    GC_LOG_SEL="$HOTSPOT_GC_LOG_COMMON,gc+humongous=debug,gc+ihop=debug,gc+remset=debug,gc+region=debug"
    ;;
  *)
    echo "ERROR: --gc must be: g1 zgc shen (HotSpot) | gencon balanced optthruput optavgpause (OpenJ9) | mix-gencon mix-balanced (mixed executors), got '$GC'"
    exit 2 ;;
esac

if [ "$CLUSTER_MANAGER" = "kubernetes" ] \
   && [ "$JVM_FAMILY" != "hotspot" ] \
   && [ -z "$IMAGE" ]; then
  echo "ERROR: OPENJ9_IMAGE is required for Kubernetes OpenJ9 or mixed-JVM runs." >&2
  echo "Build environment/Dockerfile.semeru-milestone from the official Apache Spark base, then set OPENJ9_IMAGE to that tag." >&2
  exit 2
fi

if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$JVM_FAMILY" = "mixed" ]; then
  # In Standalone mix mode, worker placement decides the JVM. Do not enable the
  # Kubernetes java shim or any global executor GC flags.
  :
fi

if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ "$JVM_FAMILY" = "mixed" ] && [ "$JITSERVER_ENABLED" = "false" ]; then
  JITSERVER_ENABLED="true"
fi

if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$JVM_FAMILY" = "mixed" ] && [ "$JITSERVER_ENABLED" = "true" ]; then
  echo "ERROR: standalone mix cannot apply --jitserver globally; configure OpenJ9 worker-local JVM settings instead." >&2
  exit 2
fi

if [ "$JITSERVER_ENABLED" = "true" ]; then
  [ "$JVM_FAMILY" = "openj9" ] || [ "$JVM_FAMILY" = "mixed" ] || {
    echo "ERROR: --jitserver true is only valid for OpenJ9/Semeru or mixed-executor GC policies" >&2
    exit 2
  }
  # Bare-metal Standalone: a fixed JITServer endpoint instead of the per-node k8s Services.
  if [ -z "$JITSERVER_ADDRESS" ] && [ "$CLUSTER_MANAGER" = "standalone" ] \
     && [ -n "${STANDALONE_JITSERVER_ADDRESS:-}" ]; then
    case "$STANDALONE_JITSERVER_ADDRESS" in
      *[!A-Za-z0-9_.:-]*)
        echo "ERROR: invalid STANDALONE_JITSERVER_ADDRESS '$STANDALONE_JITSERVER_ADDRESS'" >&2
        exit 2
        ;;
    esac
    JITSERVER_ADDRESS="$STANDALONE_JITSERVER_ADDRESS"
  fi
  if [ -z "$JITSERVER_ADDRESS" ]; then
    case "$NODE" in
      worker1) JITSERVER_ADDRESS="jitserver-worker1" ;;
      worker2) JITSERVER_ADDRESS="jitserver-worker2" ;;
      *)
        echo "ERROR: --jitserver true needs --jitserver-address unless --node is worker1 or worker2" >&2
        exit 2
        ;;
    esac
  fi
fi
if [ "$JITSERVER_AOT_CACHE_ENABLED" = "true" ]; then
  [ "$JITSERVER_ENABLED" = "true" ] || {
    echo "ERROR: --jitserver-aot-cache true requires --jitserver true" >&2
    exit 2
  }
  if [ -z "$JITSERVER_AOT_CACHE_NAME" ]; then
    JITSERVER_AOT_CACHE_NAME="spark-${BENCHMARK}-${QUERY}-${GC}"
  fi
  case "$JITSERVER_AOT_CACHE_NAME" in
    *[!A-Za-z0-9_.-]*) echo "ERROR: --jitserver-aot-cache-name must contain only letters/digits/dot/underscore/hyphen, got '$JITSERVER_AOT_CACHE_NAME'"; exit 2 ;;
  esac
fi

if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  case "$STANDALONE_POOL" in
    auto|g1|openj9) ;;
    *) echo "ERROR: --standalone-pool must be auto|g1|openj9, got '$STANDALONE_POOL'"; exit 2 ;;
  esac

  if [ "$STANDALONE_POOL" = "auto" ]; then
    case "$JVM_FAMILY" in
      hotspot) STANDALONE_POOL="g1" ;;
      openj9)  STANDALONE_POOL="openj9" ;;
    esac
  fi

  if [ "$INSTANCES_EXPLICIT" = "false" ]; then
    INSTANCES="3"
  fi
  # The persistent executor workers are sized for one 2-core executor with a
  # 3g heap plus 1g overhead. The persistent driver-only worker is sized for a
  # 4g driver heap plus 1g overhead. Explicit user values remain intentional.
  if [ "$HEAP_EXPLICIT" = "false" ]; then HEAP="3g"; fi
  if [ "$OVERHEAD_EXPLICIT" = "false" ]; then OVERHEAD="1g"; fi
  if [ "$DRIVER_MEMORY_EXPLICIT" = "false" ]; then DRIVER_MEMORY="4g"; fi
  if [ "$DRIVER_CORES_EXPLICIT" = "false" ]; then DRIVER_CORES="2"; fi
  if [ "$DRIVER_OVERHEAD_EXPLICIT" = "false" ]; then DRIVER_OVERHEAD="1g"; fi
  if [ "$STANDALONE_POOL" = "openj9" ] && [ "$CORES_EXPLICIT" = "false" ]; then
    CORES="2"
  fi

  if [ "$STANDALONE_POOL" = "g1" ] && [ "$GC" != "g1" ]; then
    echo "ERROR: standalone g1 pool is pinned to HotSpot G1 workers; use --gc g1." >&2
    echo "       ZGC/Shenandoah need separate Standalone worker pools without JAVA_TOOL_OPTIONS=-XX:+UseG1GC." >&2
    exit 2
  fi
  if [ "$STANDALONE_POOL" = "openj9" ] && [ "$GC" != "balanced" ] && [ "$GC" != "gencon" ]; then
    echo "ERROR: standalone openj9 pool runs Semeru/OpenJ9 workers; use --gc balanced or --gc gencon." >&2
    echo "       The GC policy is applied via spark.executor.extraJavaOptions, not worker JAVA_TOOL_OPTIONS." >&2
    exit 2
  fi

  case "$STANDALONE_POOL:$JVM_FAMILY" in
    g1:hotspot) ;;
    openj9:openj9) ;;
    g1:*)
      echo "ERROR: --standalone-pool g1 only supports --gc g1, got --gc $GC" >&2
      exit 2
      ;;
    openj9:*)
      echo "ERROR: --standalone-pool openj9 only supports --gc balanced, got --gc $GC" >&2
      exit 2
      ;;
  esac

  if [ "$STANDALONE_LOCAL_TUNNEL" = "true" ]; then
    case "$STANDALONE_POOL" in
      g1)  SUBMIT_MASTER="$STANDALONE_G1_TUNNEL_MASTER"; STANDALONE_LOCAL_TUNNEL_ADDRESS="$STANDALONE_G1_LOCAL_REST_ADDRESS"; STANDALONE_LOCAL_TUNNEL_HOST="spark-standalone-master-g1.spark.svc.cluster.local" ;;
      openj9) SUBMIT_MASTER="$STANDALONE_MIX_TUNNEL_MASTER"; STANDALONE_LOCAL_TUNNEL_ADDRESS="$STANDALONE_MIX_LOCAL_REST_ADDRESS"; STANDALONE_LOCAL_TUNNEL_HOST="spark-standalone-master-mix.spark.svc.cluster.local" ;;
    esac
  elif [ -z "$SUBMIT_MASTER" ] && [ -n "$STANDALONE_DEFAULT_MASTER" ]; then
    SUBMIT_MASTER="$STANDALONE_DEFAULT_MASTER"
    SUBMIT_MASTER_EXPLICIT="true"
  fi

  if [ -z "$SUBMIT_MASTER" ]; then
    case "$STANDALONE_POOL" in
      g1)  SUBMIT_MASTER="$STANDALONE_G1_MASTER" ;;
      openj9) SUBMIT_MASTER="$STANDALONE_MIX_MASTER" ;;
    esac
  fi
  case "$SUBMIT_MASTER" in
    spark://*) ;;
    *) echo "ERROR: standalone mode requires --master spark://..., got '$SUBMIT_MASTER'"; exit 2 ;;
  esac

  if [ "$SUBMIT_MASTER_EXPLICIT" = "true" ]; then
    expected_master=""
    if [ "$STANDALONE_LOCAL_TUNNEL" = "true" ]; then
      case "$STANDALONE_POOL" in
        g1) expected_master="$STANDALONE_G1_TUNNEL_MASTER" ;;
        openj9) expected_master="$STANDALONE_MIX_TUNNEL_MASTER" ;;
      esac
    else
      case "$STANDALONE_POOL" in
        g1) expected_master="$STANDALONE_G1_MASTER" ;;
        openj9) expected_master="$STANDALONE_MIX_MASTER" ;;
      esac
    fi
    if [ -n "$expected_master" ] && [ "$SUBMIT_MASTER" != "$expected_master" ]; then
      echo "ERROR: explicit standalone master '$SUBMIT_MASTER' does not match $STANDALONE_POOL pool master '$expected_master'." >&2
      echo "       Use the matching --standalone-pool, or leave --master unset for automatic routing." >&2
      exit 2
    fi
  fi

  if [ "$STANDALONE_UI_REVERSE_PROXY_URL_EXPLICIT" = "false" ]; then
    case "$STANDALONE_POOL" in
      g1)  STANDALONE_UI_REVERSE_PROXY_URL="$STANDALONE_G1_UI_REVERSE_PROXY_URL" ;;
      openj9) STANDALONE_UI_REVERSE_PROXY_URL="$STANDALONE_MIX_UI_REVERSE_PROXY_URL" ;;
    esac
  fi

  if [ "$STANDALONE_POOL" = "openj9" ]; then
    # One executor per executor worker (3 workers); any positive core count the worker
    # advertises (e.g. 2, 4, or 14 on the bare-metal Ryzen layout).
    case "$CORES" in
      ''|0|*[!0-9]*) cores_ok="false" ;;
      *) cores_ok="true" ;;
    esac
    if [ "$cores_ok" != "true" ] || [ "$INSTANCES" != "3" ]; then
      echo "ERROR: standalone openj9 pool expects --instances 3 (one executor per worker)" \
        "and a positive integer --cores." >&2
      echo "       Got --instances $INSTANCES --cores $CORES." >&2
      exit 2
    fi
    if [ "$MIXED_OPENJ9_JITSERVER_ENABLED" = "true" ] && [ -z "$JITSERVER_ADDRESS" ]; then
      JITSERVER_ADDRESS="jitserver-worker2"
    fi
    if [ -z "$MIXED_OPENJ9_SCC_NAME" ]; then
      MIXED_OPENJ9_SCC_NAME="spark-${BENCHMARK}-${QUERY}-${MIXED_OPENJ9_POLICY}-standalone"
    fi
  fi
fi

case "$STANDALONE_COLLECT" in
  kubectl|ssh) ;;
  *) echo "ERROR: --standalone-collect must be kubectl|ssh, got '$STANDALONE_COLLECT'"; exit 2 ;;
esac
if [ -n "$JITSERVER_CPU_UNIT" ]; then
  [ "$STANDALONE_COLLECT" = "ssh" ] || {
    echo "ERROR: --jitserver-cpu-unit requires --standalone-collect ssh" >&2
    exit 2
  }
  case "$JITSERVER_CPU_UNIT" in
    *[!A-Za-z0-9_.@-]*)
      echo "ERROR: invalid --jitserver-cpu-unit '$JITSERVER_CPU_UNIT'" >&2; exit 2 ;;
  esac
fi
if [ "$STANDALONE_COLLECT" = "ssh" ]; then
  [ "$CLUSTER_MANAGER" = "standalone" ] || {
    echo "ERROR: --standalone-collect ssh requires --cluster-manager standalone" >&2
    exit 2
  }
  STANDALONE_SSH_EXECUTOR_NODES="$(printf '%s' "$STANDALONE_SSH_EXECUTOR_NODES" | tr ',' ' ')"
  if [ -z "$STANDALONE_SSH_DRIVER_NODE" ] || [ -z "${STANDALONE_SSH_EXECUTOR_NODES// /}" ]; then
    echo "ERROR: --standalone-collect ssh needs --standalone-ssh-driver-node and" >&2
    echo "       --standalone-ssh-executor-nodes (or the STANDALONE_SSH_* env vars)." >&2
    exit 2
  fi
  # Executors first, then the driver node (skipped if already listed). parse-run.sh reads
  # the first printStats block of scc-*.txt, so it sees an executor cache as in kubectl mode.
  for ssh_node in $STANDALONE_SSH_EXECUTOR_NODES $STANDALONE_SSH_DRIVER_NODE; do
    case "$ssh_node" in
      *[!A-Za-z0-9_.:-]*) echo "ERROR: invalid ssh node '$ssh_node'" >&2; exit 2 ;;
    esac
    case " $STANDALONE_SSH_NODES " in
      *" $ssh_node "*) ;;
      *) STANDALONE_SSH_NODES="${STANDALONE_SSH_NODES:+$STANDALONE_SSH_NODES }$ssh_node" ;;
    esac
  done
  [ -n "$STANDALONE_WORK_DIR" ] || STANDALONE_WORK_DIR="$STANDALONE_POOL_ROOT/$STANDALONE_POOL/work"
  if [ -z "$STANDALONE_MASTER_JSON_URL" ]; then
    STANDALONE_MASTER_JSON_URL="${STANDALONE_UI_REVERSE_PROXY_URL%/}/json/"
  fi
  if [ "${DRY_RUN:-0}" != "1" ]; then
    for ssh_tool in ssh curl jq; do
      command -v "$ssh_tool" >/dev/null 2>&1 || {
        echo "ERROR: --standalone-collect ssh needs '$ssh_tool' on PATH" >&2
        exit 2
      }
    done
  fi
fi

if [ -n "$MIXED_OPENJ9_SCC_NAME" ]; then
  case "$MIXED_OPENJ9_SCC_NAME" in
    *[!A-Za-z0-9_.-]*) echo "ERROR: --mixed-openj9-scc-name must contain only letters/digits/dot/underscore/hyphen, got '$MIXED_OPENJ9_SCC_NAME'"; exit 2 ;;
  esac
fi
MIXED_OPENJ9_EFFECTIVE_AOT_ENABLED="$MIXED_OPENJ9_AOT_ENABLED"
if [ "$MIXED_OPENJ9_SCC_ENABLED" = "false" ]; then
  MIXED_OPENJ9_EFFECTIVE_AOT_ENABLED="false"
fi

JITKERNEL_TASK_CPUS=""
JITKERNEL_SPARK_CONF_ARGS=()
if [ "$BENCHMARK" = "jitkernel" ]; then
  case "$CORES" in
    ''|0|*[!0-9]*)
      echo "ERROR: --bench jitkernel requires a positive integer --cores value, got '$CORES'" >&2
      exit 2
      ;;
  esac
  JITKERNEL_TASK_CPUS="$CORES"
  for ((conf_index = 1; conf_index < ${#USER_SPARK_CONF_ARGS[@]}; conf_index += 2)); do
    user_conf="${USER_SPARK_CONF_ARGS[$conf_index]}"
    case "$user_conf" in
      spark.task.cpus=*)
        user_task_cpus="${user_conf#spark.task.cpus=}"
        if [ "$user_task_cpus" != "$JITKERNEL_TASK_CPUS" ]; then
          echo "ERROR: --bench jitkernel requires spark.task.cpus=$JITKERNEL_TASK_CPUS so each executor runs at most one task; got user override '$user_conf'" >&2
          exit 2
        fi
        ;;
    esac
  done
  JITKERNEL_SPARK_CONF_ARGS=(--conf "spark.task.cpus=$JITKERNEL_TASK_CPUS")
fi

# --- Run / pod name: {bench}-{query}-{scale}-{gc}-{aqe}-{heap}-{cores}[-r<region>][-<node>], RFC1123-safe ---
RUN_ID="${BENCHMARK}-${QUERY}-${SCALE}-${GC}-${AQE_ENABLED}-${HEAP}-${CORES}"
[ -n "$REGION" ] && RUN_ID="${RUN_ID}-r${REGION}"
[ -n "$NODE" ] && RUN_ID="${RUN_ID}-${NODE}"
[ -n "$TAG" ] && RUN_ID="${RUN_ID}-${TAG}"
RUN_ID="$(printf '%s' "$RUN_ID" | tr '[:upper:]_' '[:lower:]-')"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
# One durable folder per run (stdout, gc/, provenance, scc-{before,after}, card),
# keyed by RUN_ID+timestamp -> same key on the PVC (driver-stdout/) and locally.
RUN_FOLDER_NAME="$RUN_ID-$TIMESTAMP"
LOCAL_RUN_DIR="$LOCAL_LOG_ROOT/$RUN_FOLDER_NAME"

# --- Paths / cluster ---
SPARK_LOGS_BASE_DIR="/var/spark-logs"
GC_LOGS_DIR="/logs"
JFR_DIR="$SPARK_LOGS_BASE_DIR/jfr"
JMX_PORT="9404"
SPARK_UI_PORT="4040"
JMX_AGENT_JAR="$SPARK_LOGS_BASE_DIR/jars/jmx_prometheus_javaagent.jar"
JMX_CONFIG_PATH="/etc/jmx-exporter/jmx-exporter.yaml"
EVENT_LOGS_DIR="file:$SPARK_LOGS_BASE_DIR"
NAMESPACE="spark"
KUBERNETES_MASTER_URL=""
if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ -z "$SUBMIT_MASTER" ]; then
  KUBERNETES_MASTER_URL="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  SUBMIT_MASTER="k8s://$KUBERNETES_MASTER_URL"
fi
if [ -z "${WORKLOAD_JAR_URI:-}" ]; then
  if [ "$CLUSTER_MANAGER" = "standalone" ]; then
    WORKLOAD_JAR_URI="file://$SPARK_LOGS_BASE_DIR/jars/sql-workloads-1.0.jar"
  else
    WORKLOAD_JAR_URI="local://$SPARK_LOGS_BASE_DIR/jars/sql-workloads-1.0.jar"
  fi
fi
OBJ_STORAGE_ENDPOINT="${OBJ_STORAGE_ENDPOINT:-https://hel1.your-objectstorage.com}"

# Spark downloads the remote primary jar into the container CWD (READ-ONLY /work in the
# image) -> AccessDeniedException. Fix via pod template: point workingDir at writable /tmp.
# The templates also let Spark pods run on the tainted dataplatform workers.
# (Do NOT set runAsUser/fsGroup root — that breaks the projected SA token -> 401 from K8s API.)
DRIVER_POD_TEMPLATE=""
EXECUTOR_POD_TEMPLATE=""
MIXED_SHIM_CM=""
MIXED_SHIM_FILE=""

cleanup() {
  [ -z "${DRIVER_POD_TEMPLATE:-}" ] || rm -f "$DRIVER_POD_TEMPLATE"
  [ -z "${EXECUTOR_POD_TEMPLATE:-}" ] || rm -f "$EXECUTOR_POD_TEMPLATE"
  rm -f "${MIXED_SHIM_FILE:-}"
  if [ "${STANDALONE_LOCAL_TUNNEL_HOSTS_FILE_CREATED:-false}" = "true" ]; then
    rm -f "$STANDALONE_LOCAL_TUNNEL_HOSTS_FILE"
  fi
  if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ -n "${MIXED_SHIM_CM:-}" ] && [ "${DRY_RUN:-0}" != "1" ]; then
    kubectl delete configmap "$MIXED_SHIM_CM" -n spark --ignore-not-found >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

write_pod_template() {
  local path="$1" container_name="$2"
  local mixed_volume="" mixed_mount=""
  if [ "$container_name" = "spark-kubernetes-executor" ] && [ -n "$MIXED_SHIM_CM" ]; then
    mixed_volume="    - name: jvm-shim
      configMap:
        name: $MIXED_SHIM_CM
        defaultMode: 493"
    mixed_mount="        - name: jvm-shim
          mountPath: /opt/jvmshim/bin"
  fi

  if [ "$JMX_ENABLED" = "true" ]; then
    cat > "$path" <<YAML
apiVersion: v1
kind: Pod
spec:
  tolerations:
    - key: dataplatform
      operator: Exists
      effect: NoSchedule
  volumes:
    - name: jmx-exporter-config
      configMap:
        name: spark-jmx-exporter-config
$mixed_volume
  containers:
    - name: $container_name
      workingDir: /tmp
      volumeMounts:
        - name: jmx-exporter-config
          mountPath: /etc/jmx-exporter
          readOnly: true
$mixed_mount
YAML
  else
    if [ -n "$mixed_volume" ]; then
      cat > "$path" <<YAML
apiVersion: v1
kind: Pod
spec:
  tolerations:
    - key: dataplatform
      operator: Exists
      effect: NoSchedule
  volumes:
$mixed_volume
  containers:
    - name: $container_name
      workingDir: /tmp
      volumeMounts:
$mixed_mount
YAML
    else
      cat > "$path" <<YAML
apiVersion: v1
kind: Pod
spec:
  tolerations:
    - key: dataplatform
      operator: Exists
      effect: NoSchedule
  containers:
    - name: $container_name
      workingDir: /tmp
YAML
    fi
  fi
}

# --- Per-run observability capture helpers (Standalone pools) ----------------
# All best-effort: they only produce evidence and never affect EXIT_CODE.
find_executor_worker_pod() {
  # $1 = node (worker1|worker2). Echoes one Running executor-worker pod name.
  kubectl get pods -n "$NAMESPACE" \
    -l "research-role=spark-standalone-worker,research-node=$1" \
    --field-selector=status.phase=Running --no-headers 2>/dev/null | awk '{print $1}' | head -1
}
read_cgroup_mem_limit() {
  # $1 = node. Echoes the executor worker's cgroup memory limit in bytes.
  local pod; pod="$(find_executor_worker_pod "$1")"
  [ -n "$pod" ] || return 0
  kubectl exec -n "$NAMESPACE" "$pod" -- sh -c \
    'cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null' \
    2>/dev/null
}
capture_scc_printstats() {
  # $1 = destination file. -Xshareclasses:printStats for the run's persistent
  # SCC (cache fill, # AOT Methods, % full, % stale). OpenJ9 pool + SCC only.
  local dest="$1" pod
  [ "$OPENJ9_SCC_ENABLED" = "true" ] && [ "$STANDALONE_POOL" = "openj9" ] || return 0
  if [ "$STANDALONE_COLLECT" = "ssh" ]; then ssh_capture_scc_printstats "$dest"; return 0; fi
  pod="$(find_executor_worker_pod worker2)"
  [ -n "$pod" ] || { echo "n/a: no running openj9 worker pod" >"$dest" 2>/dev/null; return 0; }
  kubectl exec -n "$NAMESPACE" "$pod" -- sh -c \
    "/opt/java/semeru/bin/java -Xshareclasses:name=$OPENJ9_SCC_NAME,cacheDir=/scc,printStats -version 2>&1" \
    >"$dest" 2>/dev/null || echo "n/a: printStats failed for $OPENJ9_SCC_NAME" >>"$dest"
}

# --- ssh collection helpers (--standalone-collect ssh, bare-metal pools) --------
# Same run-folder layout as the kubectl helpers; also best-effort only.
node_sh() {
  # node_sh <node> <sh-script> [args...]: run the script on <node> with args as $1..$n.
  # The script goes over stdin, so only the (simple, validated) args need quoting.
  local node="$1" script="$2" args="" a
  shift 2
  if [ "$node" = "local" ]; then
    sh -s -- "$@" <<<"$script"
    return
  fi
  for a in "$@"; do args="$args $(printf '%q' "$a")"; done
  # shellcheck disable=SC2086  # STANDALONE_SSH_OPTS is a word list by design.
  ssh $STANDALONE_SSH_OPTS "$STANDALONE_SSH_USER@$node" "sh -s --$args" <<<"$script"
}
ssh_capture_scc_printstats() {
  # $1 = destination file. One "### scc-printstats" block per node (executors first).
  # printStats always exits non-zero, so success is judged from its output: the stats
  # header for this cache plus a "Cache is N% full" line.
  local dest="$1" node out rc reason
  : >"$dest"
  for node in $STANDALONE_SSH_NODES; do
    echo "### scc-printstats node=$node name=$OPENJ9_SCC_NAME cacheDir=/scc" >>"$dest"
    rc=0
    out="$(node_sh "$node" \
      '"$1" -Xshareclasses:name="$2",cacheDir=/scc,printStats -version 2>&1' \
      "$SEMERU_JAVA" "$OPENJ9_SCC_NAME" 2>/dev/null)" || rc=$?
    [ -z "$out" ] || printf '%s\n' "$out" >>"$dest"
    if grep -qF "Current statistics for cache \"$OPENJ9_SCC_NAME\"" <<<"$out" \
       && grep -qE 'Cache is [0-9]+% full' <<<"$out"; then
      continue
    fi
    reason="$(grep -m1 -oE 'JVMSHRC[0-9]+[A-Z] .*' <<<"$out" || true)"
    echo "n/a: printStats failed for $OPENJ9_SCC_NAME on $node" \
      "(${reason:-no stats in output, rc=$rc})" >>"$dest"
  done
}
ssh_pull_logs() {
  # $1 = file glob under $GC_LOGS_DIR, $2 = local dir. Pulls matches from every node;
  # a name already pulled from another node (same pid) is saved as <node>-<name>.
  local pattern="$1" dest="$2" node list src base out
  for node in $STANDALONE_SSH_NODES; do
    list="$(node_sh "$node" 'ls -d "$1"/$2 2>/dev/null' "$GC_LOGS_DIR" "$pattern" \
      2>/dev/null || true)"
    for src in $list; do
      base="$(basename "$src")"
      out="$dest/$base"
      [ -e "$out" ] && out="$dest/$node-$base"
      node_sh "$node" 'cat "$1"' "$src" >"$out" 2>/dev/null || true
    done
  done
}
ssh_persist_driver_stdout() {
  # Cluster-mode driver dir is <work dir>/<submissionId> on the driver-worker node.
  local node="$STANDALONE_SSH_DRIVER_NODE" src
  src="$(node_sh "$node" \
    'for d in "$1/$2" "$1"/*/"$2"; do [ -d "$d" ] && { echo "$d"; exit 0; }; done; exit 3' \
    "$STANDALONE_WORK_DIR" "$SUBMISSION_ID" 2>/dev/null || true)"
  if [ -z "$src" ]; then
    echo "WARN: no driver dir $STANDALONE_WORK_DIR/$SUBMISSION_ID on $node;" \
      "driver stdout not persisted" >&2
    return 0
  fi
  mkdir -p "$LOCAL_RUN_DIR"
  node_sh "$node" 'cat "$1/stdout"' "$src" >"$LOCAL_RUN_DIR/stdout" 2>/dev/null || true
  node_sh "$node" 'cat "$1/stderr"' "$src" >"$LOCAL_RUN_DIR/stderr" 2>/dev/null || true
  echo "DRIVER_STDOUT local=$LOCAL_RUN_DIR node=$node src=$src submissionid=$SUBMISSION_ID"
}
# JITServer CPU accounting (--jitserver-cpu-unit): one "node usage_usec oom_kill" line per
# ssh node from the unit's cgroup v2 files; "-" when a value is unreadable.
JITSERVER_CGROUP_SCRIPT='u="$1"; d=""
for c in "/sys/fs/cgroup/system.slice/$u" "/sys/fs/cgroup/system.slice/$u.service"; do
  [ -d "$c" ] && { d="$c"; break; }
done
us=""; oom=""
if [ -n "$d" ]; then
  us=$(awk "/^usage_usec /{print \$2}" "$d/cpu.stat" 2>/dev/null)
  oom=$(awk "/^oom_kill /{print \$2}" "$d/memory.events" 2>/dev/null)
fi
echo "${us:--} ${oom:--}"'
jitserver_cgroup_snapshot() {
  local node v
  for node in $STANDALONE_SSH_NODES; do
    v="$(node_sh "$node" "$JITSERVER_CGROUP_SCRIPT" "$JITSERVER_CPU_UNIT" 2>/dev/null \
      | tail -1 || true)"
    echo "$node ${v:-- -}"
  done
}
jitserver_cpu_provenance() {
  # Print jitserver_* provenance lines from the before/after snapshots.
  echo "jitserver_cpu_unit=$JITSERVER_CPU_UNIT"
  # Snapshots go through the environment: awk -v rejects multi-line values on some awks.
  JS_BEFORE="$JITSERVER_CPU_BEFORE" JS_AFTER="$JITSERVER_CPU_AFTER" awk '
    BEGIN {
      before = ENVIRON["JS_BEFORE"]; after = ENVIRON["JS_AFTER"]
      n = split(before, b, "\n")
      for (i = 1; i <= n; i++) { split(b[i], f, " "); bu[f[1]] = f[2]; bo[f[1]] = f[3] }
      n = split(after, a, "\n"); tot = 0; ok = 0
      for (i = 1; i <= n; i++) {
        split(a[i], f, " "); node = f[1]
        if (node == "") continue
        if (f[2] ~ /^[0-9]+$/ && bu[node] ~ /^[0-9]+$/) {
          d = (f[2] - bu[node]) / 1e6; tot += d; ok++
          printf "jitserver_cpu_s_%s=%.3f\n", node, d
        } else printf "jitserver_cpu_s_%s=n/a\n", node
        if (f[3] ~ /^[0-9]+$/ && bo[node] ~ /^[0-9]+$/)
          printf "jitserver_oom_kill_delta_%s=%d\n", node, f[3] - bo[node]
        else printf "jitserver_oom_kill_delta_%s=n/a\n", node
      }
      if (ok) printf "jitserver_cpu_s_total=%.3f\n", tot; else print "jitserver_cpu_s_total=n/a"
    }'
}
fetch_master_json() {
  # Master /json/ (activeapps + completedapps) for the APP_ID lookup.
  if [ "$STANDALONE_COLLECT" = "ssh" ]; then
    curl -fsS --max-time 30 "$STANDALONE_MASTER_JSON_URL" 2>/dev/null || true
  else
    kubectl get --raw \
      "/api/v1/namespaces/$NAMESPACE/services/http:$MASTER_SERVICE:8080/proxy/json/" \
      2>/dev/null || true
  fi
}

if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  DRIVER_POD_TEMPLATE="$(mktemp -t spark-driver-podtemplate.XXXXXX)"
  EXECUTOR_POD_TEMPLATE="$(mktemp -t spark-executor-podtemplate.XXXXXX)"

  if [ "$JVM_FAMILY" = "mixed" ]; then
    MIXED_SHIM_CM="jvm-shim-$RUN_ID"
    MIXED_SHIM_FILE="$(mktemp -t spark-mixed-jvm-shim.XXXXXX)"
    MIXED_SCC_NAME="spark-${BENCHMARK}-${QUERY}-${MIXED_OPENJ9_POLICY}-mixture"
    MIXED_OPENJ9_JITSERVER_OPTS=""
    MIXED_OPENJ9_LABEL_SUFFIX="LocalJIT-SCC"
    if [ "$MIXED_OPENJ9_JITSERVER_ENABLED" = "true" ]; then
      MIXED_OPENJ9_JITSERVER_OPTS="-XX:+UseJITServer -XX:JITServerAddress=$JITSERVER_ADDRESS -XX:+JITServerLogConnections"
      MIXED_OPENJ9_LABEL_SUFFIX="JITServer-SCC"
    fi
    MIXED_OPENJ9_SHARECLASSES_OPTS="-Xshareclasses:name=$MIXED_SCC_NAME,cacheDir=/mnt/scc,verbose,verboseAOT,nonfatal"
    MIXED_OPENJ9_AOT_OPTS="-Xaot"
    MIXED_OPENJ9_AOT_LABEL="AOT"
    if [ "$MIXED_OPENJ9_AOT_ENABLED" = "false" ]; then
      MIXED_OPENJ9_SHARECLASSES_OPTS="-Xshareclasses:name=$MIXED_SCC_NAME,cacheDir=/mnt/scc,verbose,nonfatal"
      MIXED_OPENJ9_AOT_OPTS="-Xnoaot"
      MIXED_OPENJ9_AOT_LABEL="NoAOT"
    fi
    cat > "$MIXED_SHIM_FILE" <<SH
#!/bin/sh
eid=""; prev=""
for a in "\$@"; do
  if [ "\$prev" = "--executor-id" ]; then eid="\$a"; break; fi
  prev="\$a"
done
if [ -n "\$eid" ] && [ "\$((eid % 2))" -eq 0 ]; then
  REAL=/opt/java/semeru/bin/java
  EXTRA="-Xgcpolicy:$MIXED_OPENJ9_POLICY $MIXED_OPENJ9_EXTRA_OPTS -Xverbosegclog:$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-executor-\$eid.log $MIXED_OPENJ9_JITSERVER_OPTS $MIXED_OPENJ9_SHARECLASSES_OPTS $MIXED_OPENJ9_AOT_OPTS -Xscmx$MIXED_OPENJ9_SCCMX"
  LABEL="OpenJ9-$MIXED_OPENJ9_POLICY-$MIXED_OPENJ9_LABEL_SUFFIX-$MIXED_OPENJ9_AOT_LABEL"
elif [ -n "\$eid" ]; then
  REAL=/opt/java/openjdk/bin/java
  EXTRA="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+PrintCommandLineFlags -XX:+PrintFlagsFinal -Xlog:$GC_LOG_SEL:file=$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-executor-\$eid.log:utctime,uptime,level,tags:filecount=1,filesize=200m"
  LABEL="HotSpot-G1"
else
  REAL=/opt/java/openjdk/bin/java
  EXTRA=""
  LABEL="default-noeid"
fi
echo "JVM-SHIM: executor-id=\$eid -> \$REAL [\$LABEL]" >&2
exec "\$REAL" \$EXTRA "\$@"
SH
    if [ "${DRY_RUN:-0}" != "1" ]; then
      kubectl delete configmap "$MIXED_SHIM_CM" -n spark --ignore-not-found >/dev/null 2>&1 || true
      kubectl create configmap "$MIXED_SHIM_CM" -n spark --from-file=java="$MIXED_SHIM_FILE" >/dev/null
    fi
  fi

  write_pod_template "$DRIVER_POD_TEMPLATE" spark-kubernetes-driver
  write_pod_template "$EXECUTOR_POD_TEMPLATE" spark-kubernetes-executor
fi

NEEDS_S3_SECRET=0
case "$DATA_LOCATION $EVENT_LOGS_DIR $WORKLOAD_JAR_URI" in
  *s3://*|*s3a://*) NEEDS_S3_SECRET=1 ;;
esac
S3_SECRET_ARGS=()
if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ "$NEEDS_S3_SECRET" -eq 1 ] && kubectl get secret s3-creds -n "$NAMESPACE" >/dev/null 2>&1; then
  S3_SECRET_ARGS=(
    --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=s3-creds:AWS_ACCESS_KEY_ID
    --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-creds:AWS_SECRET_ACCESS_KEY
    --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=s3-creds:AWS_ACCESS_KEY_ID
    --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-creds:AWS_SECRET_ACCESS_KEY
  )
elif [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ "$NEEDS_S3_SECRET" -eq 1 ]; then
  echo "ERROR: this run uses s3:// or s3a:// but secret spark/s3-creds does not exist" >&2
  exit 2
fi

ensure_shared_log_dirs() {
  local pod
  pod="$(
    kubectl get pods -n "$NAMESPACE" -l app=spark-history-server-worker4 \
      --field-selector=status.phase=Running \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
  )"
  [ -n "$pod" ] || return 0
  kubectl exec -n "$NAMESPACE" "$pod" -- sh -lc \
    'mkdir -p /event-logs/gc-logs-raw /event-logs/jfr /event-logs/jars && chmod 2777 /event-logs/gc-logs-raw /event-logs/jfr /event-logs/jars' \
    >/dev/null 2>&1 || true
}
if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  ensure_shared_log_dirs
fi

check_jmx_assets() {
  [ "$JMX_ENABLED" = "true" ] || return 0
  kubectl get configmap spark-jmx-exporter-config -n "$NAMESPACE" >/dev/null 2>&1 || {
    echo "ERROR: --jmx true requires configmap $NAMESPACE/spark-jmx-exporter-config" >&2
    echo "       Apply research-related/scripts/k8s/spark-jmx-exporter-config.yaml first." >&2
    exit 2
  }

  local pod
  pod="$(
    kubectl get pods -n "$NAMESPACE" -l app=spark-history-server-worker4 \
      --field-selector=status.phase=Running \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
  )"
  [ -n "$pod" ] || {
    echo "ERROR: --jmx true needs a running history-server pod to verify the shared JMX agent jar" >&2
    exit 2
  }
  kubectl exec -n "$NAMESPACE" "$pod" -- sh -lc 'test -s /event-logs/jars/jmx_prometheus_javaagent.jar' >/dev/null 2>&1 || {
    echo "ERROR: --jmx true requires /mnt/sparklogs/jars/jmx_prometheus_javaagent.jar" >&2
    echo "       Copy the Prometheus JMX exporter javaagent jar to the spark logs PVC first." >&2
    exit 2
  }
}
[ "$CLUSTER_MANAGER" != "kubernetes" ] || [ "${DRY_RUN:-0}" = "1" ] || check_jmx_assets

if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ -n "$NODE" ]; then
  kubectl get node "$NODE" >/dev/null 2>&1 || { echo "ERROR: Kubernetes node not found: $NODE"; exit 2; }
elif [ "$CLUSTER_MANAGER" = "standalone" ] && [ -n "$NODE" ]; then
  echo "WARN: --node does not pin Standalone executors; it is kept only in the run id and for JITServer address auto-selection." >&2
fi

STANDALONE_MAX_CORES=""
if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  case "$CORES" in ''|*[!0-9]*) echo "ERROR: standalone mode requires numeric --cores, got '$CORES'"; exit 2 ;; esac
  case "$INSTANCES" in ''|*[!0-9]*) echo "ERROR: standalone mode requires numeric --instances, got '$INSTANCES'"; exit 2 ;; esac
  STANDALONE_MAX_CORES=$((CORES * INSTANCES))
fi

if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$STANDALONE_LOCAL_TUNNEL" = "true" ]; then
  STANDALONE_LOCAL_TUNNEL_HOSTS_FILE="$(mktemp "${TMPDIR:-/tmp}/heterojvm-spark-hosts.XXXXXX")"
  STANDALONE_LOCAL_TUNNEL_HOSTS_FILE_CREATED="true"
  printf '%s %s\n' "$STANDALONE_LOCAL_TUNNEL_ADDRESS" "$STANDALONE_LOCAL_TUNNEL_HOST" > "$STANDALONE_LOCAL_TUNNEL_HOSTS_FILE"

  STANDALONE_HOSTS_OPT="-Djdk.net.hosts.file=$STANDALONE_LOCAL_TUNNEL_HOSTS_FILE"
  export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:+$SPARK_SUBMIT_OPTS }$STANDALONE_HOSTS_OPT"

  if [ "${DRY_RUN:-0}" != "1" ] && command -v nc >/dev/null 2>&1; then
    if ! nc -z "$STANDALONE_LOCAL_TUNNEL_ADDRESS" 6066 >/dev/null 2>&1; then
      echo "ERROR: standalone local tunnel is enabled, but $STANDALONE_LOCAL_TUNNEL_ADDRESS:6066 is not reachable." >&2
      echo "       In a separate terminal, run: research-related/scripts/spark_submit/port-forward-standalone-masters.sh $STANDALONE_POOL" >&2
      exit 2
    fi
  fi
fi

# The local RestSubmissionClient creates a short-lived RPC environment before
# sending a cluster-mode request. On submit hosts whose hostname resolves to a
# non-local address, force that client-only environment to loopback. The REST
# server filters SPARK_LOCAL_IP from the submitted application's environment.
if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
fi

# --- Print experiment config ---
echo "=============================================="
echo "GC Screening: $RUN_ID"
echo "=============================================="
echo "  Cluster manager:    $CLUSTER_MANAGER"
echo "  Submit master:      $SUBMIT_MASTER"
echo "  Query:              $QUERY ($BENCHMARK SF$SCALE)"
if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$JVM_FAMILY" = "mixed" ]; then
  echo "  GC:                 $GC  [$JVM_FAMILY]  worker-local JVM settings"
else
  echo "  GC:                 $GC  [$JVM_FAMILY]  $GC_OPTS"
fi
if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  echo "  Image:              worker-pool managed"
  echo "  Standalone pool:    $STANDALONE_POOL"
else
  echo "  Image:              $IMAGE"
fi
echo "  AQE:                $AQE_ENABLED"
echo "  Heap / overhead:    $HEAP / ${OVERHEAD:-<spark-default>}"
echo "  Cores x instances:  $CORES x $INSTANCES"
if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  echo "  CPU req/limit:      executor $EXECUTOR_CPU_REQUEST/$EXECUTOR_CPU_LIMIT, driver $DRIVER_CPU_REQUEST/$DRIVER_CPU_LIMIT"
else
  echo "  Standalone max:     spark.cores.max=$STANDALONE_MAX_CORES"
  echo "  Placement resources: driver heterojvm_driver=1; executor heterojvm_executor=1"
fi
echo "  Driver mem/overhd:  $DRIVER_MEMORY / ${DRIVER_OVERHEAD:-<spark-default>}"
echo "  Driver cores:       $DRIVER_CORES"
echo "  Broadcast thr:      ${BC:-<spark-default 10MB, conf omitted>}  (off=-1)"
echo "  G1 region size:     ${REGION:-<ergonomic>}"
echo "  Node pin:           ${NODE:-<none>}"
if [ "$JVM_FAMILY" = "mixed" ]; then
  echo "  JITServer:          OpenJ9 executors only; see Mixed OpenJ9"
elif [ "$JITSERVER_ENABLED" = "true" ]; then
  echo "  JITServer:          true  address=$JITSERVER_ADDRESS"
  if [ "$JITSERVER_AOT_CACHE_ENABLED" = "true" ]; then
    echo "  JITServer AOT:      true  cache=$JITSERVER_AOT_CACHE_NAME"
  else
    echo "  JITServer AOT:      false"
  fi
else
  echo "  JITServer:          false"
fi
if [ "$JVM_FAMILY" = "mixed" ]; then
  if [ "$CLUSTER_MANAGER" = "standalone" ]; then
    echo "  JVM mix:            worker2 pool: 1 HotSpot/G1 worker + 2 OpenJ9/$MIXED_OPENJ9_POLICY workers"
    echo "  Mixed SCC:          enabled=$MIXED_OPENJ9_SCC_ENABLED name=${MIXED_OPENJ9_SCC_NAME:-<not-applicable>} cacheDir=/mnt/scc on OpenJ9 workers"
    echo "  Mixed OpenJ9:       jitserver=$MIXED_OPENJ9_JITSERVER_ENABLED sccmx=$MIXED_OPENJ9_SCCMX aot_requested=$MIXED_OPENJ9_AOT_ENABLED aot_effective=$MIXED_OPENJ9_EFFECTIVE_AOT_ENABLED via /opt/jvmshim"
  else
    echo "  JVM mix:            $JVM_MIX  (odd executors HotSpot/G1, even executors OpenJ9/$MIXED_OPENJ9_POLICY)"
    echo "  Mixed SCC:          /mnt/scc  name=$MIXED_SCC_NAME"
    echo "  Mixed OpenJ9:       jitserver=$MIXED_OPENJ9_JITSERVER_ENABLED sccmx=$MIXED_OPENJ9_SCCMX aot=$MIXED_OPENJ9_AOT_ENABLED"
  fi
fi
if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  echo "  Toleration:         dataplatform:NoSchedule"
fi
if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  echo "  UI reverse proxy:   $STANDALONE_UI_REVERSE_PROXY_URL"
  echo "  Local REST tunnel:  $STANDALONE_LOCAL_TUNNEL"
  if [ "$STANDALONE_COLLECT" = "ssh" ]; then
    echo "  Evidence collect:   ssh"
    echo "  ssh nodes:          $STANDALONE_SSH_NODES (driver $STANDALONE_SSH_DRIVER_NODE)"
    echo "  Worker work dir:    $STANDALONE_WORK_DIR"
  fi
  if [ "$STANDALONE_LOCAL_TUNNEL" = "true" ]; then
    echo "  Hosts override:     $STANDALONE_LOCAL_TUNNEL_HOSTS_FILE"
    echo "  Hosts mapping:      $STANDALONE_LOCAL_TUNNEL_ADDRESS $STANDALONE_LOCAL_TUNNEL_HOST"
    echo "  Local endpoint:     $SUBMIT_MASTER"
  fi
fi
echo "  Event log dir:      $EVENT_LOGS_DIR"
if [ "$JMX_ENABLED" = "true" ]; then
  echo "  JMX exporter:       true  port=$JMX_PORT"
else
  echo "  JMX exporter:       false"
fi
if [ "$JVM_FAMILY" = "hotspot" ]; then
  echo "  JFR:                $JFR_ENABLED"
else
  echo "  JFR:                skipped for OpenJ9"
fi
echo "  Workload jar:       $WORKLOAD_JAR_URI"
echo "  Data location:      $DATA_LOCATION"
if [ "${#USER_SPARK_CONF_ARGS[@]}" -gt 0 ]; then
  echo "  Extra Spark conf:   ${USER_SPARK_CONF_ARGS[*]}"
else
  echo "  Extra Spark conf:   <none>"
fi
echo "  Timestamp:          $TIMESTAMP"
echo "=============================================="

# --- Per-JVM GC logging flags (HotSpot -Xlog vs OpenJ9 -Xverbosegclog) ---
if [ "$JVM_FAMILY" = "openj9" ]; then
  # GC policy comes from --gc (gencon|balanced|...), never hardcoded, so
  # arraylet (balanced) can be isolated from SCC. /logs is the per-worker
  # gc-logs-raw NFS dir (verified on the live worker mount). SCC lives on the
  # persistent /scc hostPath so the cache survives across submits.
  OPENJ9_SCC_OPTS=""
  if [ "$OPENJ9_SCC_ENABLED" = "true" ]; then
    OPENJ9_SCC_OPTS="-Xshareclasses:name=$OPENJ9_SCC_NAME,cacheDir=/scc,persistent -Xscmx$OPENJ9_SCCMX"
  fi
  OPENJ9_GC_LOG="/logs/$TIMESTAMP-$RUN_ID-%pid.log"
  # AOT/JIT warmup evidence (opt-in): compile-trace vlog lets parse-run.sh count
  # "+ (AOT load)" (warm SCC hits) vs "+ (cold)"/"+ (AOT warm)" fresh compiles.
  # OpenJ9 appends ".<date>.<time>.<pid>" to the vlog path, so it does NOT match
  # the gc "*.log" glob and is pulled separately into the run folder's jit/.
  OPENJ9_JIT_VLOG_OPTS=""
  if [ "$WARMUP_VERBOSE" = "true" ]; then
    OPENJ9_JIT_VLOG_OPTS="-Xjit:verbose={$WARMUP_VERBOSE_JIT},vlog=/logs/$TIMESTAMP-$RUN_ID-jit"
  fi
  # Extra OpenJ9 opts (e.g. -Xaot:enableSVMDuringStartup) on BOTH driver+executor.
  # -Xgcpolicy stays out of this (comes from $GC_OPTS) so it appears exactly once.
  DRIVER_JAVA_OPTS="$GC_OPTS $OPENJ9_SCC_OPTS -Xverbosegclog:$OPENJ9_GC_LOG $OPENJ9_JIT_VLOG_OPTS $OPENJ9_EXTRA_OPTS"
  EXECUTOR_JAVA_OPTS="$GC_OPTS $OPENJ9_SCC_OPTS -Xverbosegclog:$OPENJ9_GC_LOG $OPENJ9_JIT_VLOG_OPTS $OPENJ9_EXTRA_OPTS"
elif [ "$JVM_FAMILY" = "mixed" ]; then
  if [ "$CLUSTER_MANAGER" = "standalone" ]; then
    DRIVER_JAVA_OPTS=""
  else
    DRIVER_JAVA_OPTS="$GC_OPTS -XX:+UnlockDiagnosticVMOptions -XX:+PrintCommandLineFlags -XX:+PrintFlagsFinal -Xlog:$GC_LOG_SEL:file=$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-driver.log:utctime,uptime,level,tags:filecount=1,filesize=200m"
  fi
  EXECUTOR_JAVA_OPTS=""
else
  # /logs is the per-worker gc-logs-raw NFS dir; tag-aware filename keeps reruns
  # distinguishable. The gc+init line records the actual G1 region size.
  # $GC_LOG_SEL (set per-collector above) upgrades bare gc* to region+humongous+
  # heap+phases detail so the fragmentation signals are captured: "Humongous
  # regions: X->Y" (gc+heap=debug), "to-space exhausted"/"Evacuation Failure"
  # (gc* info/warning), "Pause Full" (gc*). Verified accepted by Temurin 21.0.11.
  HOTSPOT_GC_LOG="/logs/$TIMESTAMP-$RUN_ID-%p.log"
  DRIVER_JAVA_OPTS="$GC_OPTS -Xlog:$GC_LOG_SEL:file=$HOTSPOT_GC_LOG:utctime,uptime,level,tags"
  EXECUTOR_JAVA_OPTS="$GC_OPTS -Xlog:$GC_LOG_SEL:file=$HOTSPOT_GC_LOG:utctime,uptime,level,tags"
  # HotSpot JIT-tax evidence (opt-in, SAME --warmup-verbose gate as OpenJ9 verbose
  # AOT). -XX:+CITime prints total C1/C2 compile seconds at JVM exit ("Total
  # compilation time : N s") to each JVM's stdout (driver stdout is captured in the
  # run folder; executor CITime lands in the standalone worker's stdout, not pulled).
  # -Xlog:jit+compilation=debug writes one line per compiled method to a file in
  # jit/, so the count (and the heavy UDF getting compiled) is visible. The file is
  # "*-jit.<pid>.hotspot" (NOT "*.log") so it lands in jit/ like the OpenJ9 vlog,
  # not swept into gc/. Both flags are {product} on Temurin 21.0.11 (verified live).
  if [ "$WARMUP_VERBOSE" = "true" ]; then
    HOTSPOT_JIT_LOG="/logs/$TIMESTAMP-$RUN_ID-jit.%p.hotspot"
    HOTSPOT_JIT_OPTS="-XX:+CITime -Xlog:jit+compilation=debug:file=$HOTSPOT_JIT_LOG:uptime,level,tags"
    DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS $HOTSPOT_JIT_OPTS"
    EXECUTOR_JAVA_OPTS="$EXECUTOR_JAVA_OPTS $HOTSPOT_JIT_OPTS"
  fi
  if [ "$JFR_ENABLED" = "true" ]; then
    DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS -XX:StartFlightRecording=filename=$JFR_DIR/$TIMESTAMP-$RUN_ID-driver.jfr,settings=profile,disk=true,dumponexit=true,maxsize=512m"
    EXECUTOR_JAVA_OPTS="$EXECUTOR_JAVA_OPTS -XX:StartFlightRecording=filename=$JFR_DIR/$TIMESTAMP-$RUN_ID-executor-{{EXECUTOR_ID}}.jfr,settings=profile,disk=true,dumponexit=true,maxsize=512m"
  fi
fi

if [ "$JMX_ENABLED" = "true" ]; then
  JMX_JAVA_AGENT="-javaagent:$JMX_AGENT_JAR=$JMX_PORT:$JMX_CONFIG_PATH"
  DRIVER_JAVA_OPTS="$JMX_JAVA_AGENT $DRIVER_JAVA_OPTS"
  if [ -n "$EXECUTOR_JAVA_OPTS" ]; then
    EXECUTOR_JAVA_OPTS="$JMX_JAVA_AGENT $EXECUTOR_JAVA_OPTS"
  else
    EXECUTOR_JAVA_OPTS="$JMX_JAVA_AGENT"
  fi
fi

if [ "$JITSERVER_ENABLED" = "true" ] && [ "$JVM_FAMILY" != "mixed" ]; then
  JITSERVER_OPTS="-XX:+UseJITServer -XX:JITServerAddress=$JITSERVER_ADDRESS -XX:JITServerPort=$JITSERVER_PORT -XX:+JITServerLogConnections"
  if [ "$JITSERVER_AOT_CACHE_ENABLED" = "true" ]; then
    JITSERVER_OPTS="$JITSERVER_OPTS -XX:+JITServerUseAOTCache -XX:JITServerAOTCacheName=$JITSERVER_AOT_CACHE_NAME"
  fi
  DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS $JITSERVER_OPTS"
  EXECUTOR_JAVA_OPTS="$EXECUTOR_JAVA_OPTS $JITSERVER_OPTS"
fi

# Append caller-supplied extra executor JVM opts (e.g. -XX:NativeMemoryTracking=summary)
if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$JVM_FAMILY" = "mixed" ] && [ -n "$EXTRA_EXEC_OPTS" ]; then
  echo "WARN: standalone mix applies --extra-exec-opts to every executor; avoid JVM-specific flags here." >&2
fi
[ -n "$EXTRA_EXEC_OPTS" ] && EXECUTOR_JAVA_OPTS="$EXECUTOR_JAVA_OPTS $EXTRA_EXEC_OPTS"

if [ "${DRY_RUN:-0}" = "1" ]; then
  echo "DRY_RUN: image=$IMAGE  family=$JVM_FAMILY  run_id=$RUN_ID"
  echo "DRY_RUN: cluster_manager=$CLUSTER_MANAGER master=$SUBMIT_MASTER"
  echo "DRY_RUN: main_class=$MAIN_CLASS"
  echo "DRY_RUN: app_args=query=$QUERY scale=$SCALE data_location=$DATA_LOCATION"
  echo "DRY_RUN: heap=$HEAP overhead=${OVERHEAD:-<spark-default>} cores=$CORES instances=$INSTANCES driver_mem=$DRIVER_MEMORY driver_overhead=${DRIVER_OVERHEAD:-<spark-default>} driver_cores=$DRIVER_CORES broadcast=${BC:-<spark-default 10MB>} aqe=$AQE_ENABLED node=${NODE:-<none>}"
  echo "DRY_RUN: sql_conf adaptive.enabled=$AQE_ENABLED coalescePartitions.enabled=true skewJoin.enabled=$([ "$AQE_ENABLED" = "false" ] && echo "false" || echo "<omitted; spark-default true>") autoBroadcastJoinThreshold=${BC:-<omitted; spark-default 10MB>}"
  if [ "$BENCHMARK" = "jitkernel" ]; then
    echo "DRY_RUN: spark.task.cpus=$JITKERNEL_TASK_CPUS expected_tasks_per_executor=1 executor_cores=$CORES"
  fi
  if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
    echo "DRY_RUN: k8s_cpu executor_request=$EXECUTOR_CPU_REQUEST executor_limit=$EXECUTOR_CPU_LIMIT driver_request=$DRIVER_CPU_REQUEST driver_limit=$DRIVER_CPU_LIMIT"
  else
    echo "DRY_RUN: standalone_pool=$STANDALONE_POOL"
    echo "DRY_RUN: standalone_local_tunnel=$STANDALONE_LOCAL_TUNNEL"
    if [ "$STANDALONE_LOCAL_TUNNEL" = "true" ]; then
      echo "DRY_RUN: standalone_hosts_file=$STANDALONE_LOCAL_TUNNEL_HOSTS_FILE"
      echo "DRY_RUN: standalone_hosts_mapping=$STANDALONE_LOCAL_TUNNEL_ADDRESS $STANDALONE_LOCAL_TUNNEL_HOST"
      echo "DRY_RUN: standalone_local_tunnel_endpoint=$SUBMIT_MASTER"
      echo "DRY_RUN: spark_submit_opts=$SPARK_SUBMIT_OPTS"
    fi
    echo "DRY_RUN: standalone_ui_reverse_proxy=$STANDALONE_UI_REVERSE_PROXY_URL"
    if [ "$STANDALONE_COLLECT" = "ssh" ]; then
      echo "DRY_RUN: standalone_collect=ssh"
      echo "DRY_RUN: ssh_nodes=$STANDALONE_SSH_NODES user=$STANDALONE_SSH_USER" \
        "opts=$STANDALONE_SSH_OPTS"
      echo "DRY_RUN: app_id_lookup=$STANDALONE_MASTER_JSON_URL"
      echo "DRY_RUN: driver_stdout_capture=on" \
        "from=$STANDALONE_SSH_DRIVER_NODE:$STANDALONE_WORK_DIR/<submissionId>/{stdout,stderr}" \
        "local=$LOCAL_RUN_DIR"
      echo "DRY_RUN: gc_log_capture=on from=<each ssh node>:$GC_LOGS_DIR/$TIMESTAMP-$RUN_ID-*.log" \
        "to=$RUN_FOLDER_NAME/gc/"
    else
    dry_run_driver_node="worker1"; [ "$STANDALONE_POOL" = "openj9" ] && dry_run_driver_node="worker2"
    echo "DRY_RUN: driver_stdout_capture=on from=$dry_run_driver_node:/tmp/spark-work/*/<submissionId>/stdout durable=/var/spark-logs/driver-stdout/$RUN_ID-$TIMESTAMP local=$LOCAL_LOG_ROOT/$RUN_ID-$TIMESTAMP"
    echo "DRY_RUN: gc_log_capture=on from=/var/spark-logs/gc-logs-raw/*/$TIMESTAMP-$RUN_ID-*.log to=$RUN_FOLDER_NAME/gc/ (durable PVC + local pull)"
    fi
    echo "DRY_RUN: provenance_capture=on file=$RUN_FOLDER_NAME/provenance.txt (effective exec/driver extraJavaOptions, cgroup_mem_limit, heap/overhead; g1 region size read from gc log by parse-run.sh)"
    if [ "$STANDALONE_POOL" = "openj9" ] && [ "$OPENJ9_SCC_ENABLED" = "true" ]; then
      echo "DRY_RUN: scc_stats_capture=on -Xshareclasses:printStats name=$OPENJ9_SCC_NAME before(pre-submit)+after(post-run) to=$RUN_FOLDER_NAME/scc-{before,after}.txt"
    else
      echo "DRY_RUN: scc_stats_capture=off (pool=$STANDALONE_POOL scc_enabled=$OPENJ9_SCC_ENABLED)"
    fi
    echo "DRY_RUN: standalone_spark_cores_max=$STANDALONE_MAX_CORES"
    echo "DRY_RUN: standalone_placement_resources=driver:heterojvm_driver=1,executor:heterojvm_executor=1"
    echo "DRY_RUN: standalone_resource_requests=driver:heterojvm_driver=1,executor:heterojvm_executor=1"
    if [ "$STANDALONE_POOL" = "mix" ]; then
      dry_run_openj9_extra_opts="<none>"
      if [ -n "$MIXED_OPENJ9_EXTRA_OPTS" ]; then
        dry_run_openj9_extra_opts="<provided-redacted>"
      fi
      echo "DRY_RUN: standalone_mix_executor_java_home=/opt/jvmshim"
      echo "DRY_RUN: standalone_mix_executor_env=OPENJ9_GC_POLICY=$MIXED_OPENJ9_POLICY OPENJ9_SCC_ENABLED=$MIXED_OPENJ9_SCC_ENABLED OPENJ9_SCC_NAME=${MIXED_OPENJ9_SCC_NAME:-<not-applicable>} OPENJ9_SCCMX=$MIXED_OPENJ9_SCCMX OPENJ9_AOT_ENABLED=$MIXED_OPENJ9_EFFECTIVE_AOT_ENABLED OPENJ9_JITSERVER_ENABLED=$MIXED_OPENJ9_JITSERVER_ENABLED OPENJ9_JITSERVER_ADDRESS=${JITSERVER_ADDRESS:-<none>} OPENJ9_EXTRA_OPTS=$dry_run_openj9_extra_opts"
    fi
  fi
  if [ "$JVM_FAMILY" = "mixed" ]; then
    echo "DRY_RUN: jitserver=openj9-executors-only mixed_openj9_jitserver=$MIXED_OPENJ9_JITSERVER_ENABLED address=$JITSERVER_ADDRESS"
  else
    echo "DRY_RUN: jitserver=$JITSERVER_ENABLED address=${JITSERVER_ADDRESS:-<none>} port=$JITSERVER_PORT aot_cache=$JITSERVER_AOT_CACHE_ENABLED aot_cache_name=${JITSERVER_AOT_CACHE_NAME:-<none>}"
  fi
  echo "DRY_RUN: driver_opts=$DRIVER_JAVA_OPTS"
  echo "DRY_RUN: exec_opts=$EXECUTOR_JAVA_OPTS"
  echo "DRY_RUN: user_spark_conf=${USER_SPARK_CONF_ARGS[*]:-<none>}"
  echo "DRY_RUN: wallclock_capture=on submit->terminal seconds -> provenance.txt wallclock_s (all arms, includes JVM boot)"
  if [ "$JVM_FAMILY" = "openj9" ]; then
    echo "DRY_RUN: warmup_verbose=$WARMUP_VERBOSE jit_vlog=$([ "$WARMUP_VERBOSE" = "true" ] && echo "/logs/$TIMESTAMP-$RUN_ID-jit.<date>.<time>.<pid> -> $RUN_FOLDER_NAME/jit/" || echo "<off, clean timing run>")"
  elif [ "$JVM_FAMILY" = "hotspot" ]; then
    echo "DRY_RUN: warmup_verbose=$WARMUP_VERBOSE hotspot_jit=$([ "$WARMUP_VERBOSE" = "true" ] && echo "-XX:+CITime (total compile s -> driver stdout) + -Xlog:jit+compilation -> /logs/$TIMESTAMP-$RUN_ID-jit.<pid>.hotspot -> $RUN_FOLDER_NAME/jit/" || echo "<off, clean timing run>")"
  else
    echo "DRY_RUN: warmup_verbose=n/a"
  fi
  exit 0
fi

JMX_SPARK_CONF_ARGS=()
if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ "$JMX_ENABLED" = "true" ]; then
  JMX_SPARK_CONF_ARGS=(
    --conf spark.kubernetes.driver.annotation.prometheus.io/scrape=true
    --conf spark.kubernetes.driver.annotation.prometheus.io/port=$JMX_PORT
    --conf spark.kubernetes.driver.annotation.prometheus.io/path=/metrics
    --conf spark.kubernetes.executor.annotation.prometheus.io/scrape=true
    --conf spark.kubernetes.executor.annotation.prometheus.io/port=$JMX_PORT
    --conf spark.kubernetes.executor.annotation.prometheus.io/path=/metrics
    --conf spark.kubernetes.driver.label.research-jvm=$JVM_FAMILY
    --conf spark.kubernetes.driver.label.research-gc=$GC
    --conf spark.kubernetes.driver.label.research-jitserver=$JITSERVER_ENABLED
    --conf spark.kubernetes.driver.label.research-benchmark=$BENCHMARK
    --conf spark.kubernetes.driver.label.research-query=$QUERY
    --conf spark.kubernetes.driver.label.research-scale=$SCALE
    --conf spark.kubernetes.driver.label.research-role=driver
    --conf spark.kubernetes.driver.label.research-spark-prometheus=true
    --conf spark.kubernetes.executor.label.research-jvm=$JVM_FAMILY
    --conf spark.kubernetes.executor.label.research-gc=$GC
    --conf spark.kubernetes.executor.label.research-jitserver=$JITSERVER_ENABLED
	    --conf spark.kubernetes.executor.label.research-benchmark=$BENCHMARK
	    --conf spark.kubernetes.executor.label.research-query=$QUERY
	    --conf spark.kubernetes.executor.label.research-scale=$SCALE
	    --conf spark.kubernetes.executor.label.research-role=executor
	  )
fi

MIXED_SPARK_CONF_ARGS=()
if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ "$JVM_FAMILY" = "mixed" ]; then
  MIXED_SPARK_CONF_ARGS=(
    --conf spark.kubernetes.driverEnv.JAVA_HOME=/opt/java/openjdk
    --conf spark.executorEnv.JAVA_HOME=/opt/jvmshim
    --conf spark.kubernetes.executor.volumes.hostPath.scc.mount.path=/mnt/scc
    --conf spark.kubernetes.executor.volumes.hostPath.scc.options.path=/mnt/scc
    --conf spark.kubernetes.executor.volumes.hostPath.scc.options.type=DirectoryOrCreate
  )
fi

if [ "$CLUSTER_MANAGER" = "kubernetes" ] && [ "$JVM_FAMILY" = "openj9" ] && [ "$OPENJ9_LOCAL_AOT" = "true" ]; then
  MIXED_SPARK_CONF_ARGS+=(
    --conf spark.kubernetes.executor.volumes.hostPath.scc.mount.path=/mnt/scc
    --conf spark.kubernetes.executor.volumes.hostPath.scc.options.path=/mnt/scc
    --conf spark.kubernetes.executor.volumes.hostPath.scc.options.type=DirectoryOrCreate
  )
fi

STANDALONE_MIX_SPARK_CONF_ARGS=()
if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$STANDALONE_POOL" = "openj9" ]; then
  STANDALONE_MIX_SPARK_CONF_ARGS=(
    --conf spark.executorEnv.JAVA_HOME=/opt/java/semeru
  )
fi

MEMORY_OVERHEAD_SPARK_CONF_ARGS=()
if [ -n "$DRIVER_OVERHEAD" ]; then
  MEMORY_OVERHEAD_SPARK_CONF_ARGS+=(--conf spark.driver.memoryOverhead=$DRIVER_OVERHEAD)
fi
if [ -n "$OVERHEAD" ]; then
  MEMORY_OVERHEAD_SPARK_CONF_ARGS+=(--conf spark.executor.memoryOverhead=$OVERHEAD)
fi

COMMON_SPARK_CONF_ARGS=(
  --conf "spark.app.name=$RUN_ID"
  --conf spark.eventLog.enabled=true
  --conf "spark.eventLog.dir=$EVENT_LOGS_DIR"
  --conf "spark.ui.port=$SPARK_UI_PORT"
  --conf spark.ui.prometheus.enabled=true
  --conf "spark.driver.extraJavaOptions=$DRIVER_JAVA_OPTS"
  --conf "spark.executor.extraJavaOptions=$EXECUTOR_JAVA_OPTS"
  --conf "spark.driver.memory=$DRIVER_MEMORY"
  --conf "spark.driver.cores=$DRIVER_CORES"
  --conf "spark.executor.instances=$INSTANCES"
  --conf spark.dynamicAllocation.enabled=false
  --conf spark.memory.offHeap.enabled=false
  --conf spark.memory.offHeap.size=0
  --conf "spark.executor.memory=$HEAP"
  --conf "spark.executor.cores=$CORES"
)

HADOOP_SPARK_CONF_ARGS=(
  --conf "spark.hadoop.fs.s3a.endpoint=$OBJ_STORAGE_ENDPOINT"
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  --conf spark.hadoop.fs.s3a.path.style.access=true
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true
)

SQL_SPARK_CONF_ARGS=(
  --conf "spark.sql.adaptive.enabled=$AQE_ENABLED"
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.shuffle.compress=true
)
# Broadcast: emit the threshold only when a value is set (BC=""=Spark default 10MB).
if [ -n "$BC" ]; then
  SQL_SPARK_CONF_ARGS+=(
    --conf "spark.sql.adaptive.autoBroadcastJoinThreshold=$BC"
    --conf "spark.sql.autoBroadcastJoinThreshold=$BC"
  )
fi
# skewJoin: force-off only for the AQE-off GC-isolation arms. With AQE on, leave
# the Spark default (true) untouched so standard-TPC-H runs at pure defaults.
if [ "$AQE_ENABLED" = "false" ]; then
  SQL_SPARK_CONF_ARGS+=(--conf spark.sql.adaptive.skewJoin.enabled=false)
fi

K8S_SPARK_CONF_ARGS=()
STANDALONE_SPARK_CONF_ARGS=()
if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  K8S_SPARK_CONF_ARGS=(
    --conf "spark.kubernetes.namespace=$NAMESPACE"
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
    --conf "spark.kubernetes.container.image=$IMAGE"
    --conf "spark.kubernetes.driver.podTemplateFile=$DRIVER_POD_TEMPLATE"
    --conf "spark.kubernetes.executor.podTemplateFile=$EXECUTOR_POD_TEMPLATE"
    --conf "spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=$SPARK_LOGS_BASE_DIR"
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc
    --conf "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=$SPARK_LOGS_BASE_DIR"
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc
    --conf "spark.kubernetes.driver.request.cores=$DRIVER_CPU_REQUEST"
    --conf "spark.kubernetes.driver.limit.cores=$DRIVER_CPU_LIMIT"
    --conf "spark.kubernetes.executor.request.cores=$EXECUTOR_CPU_REQUEST"
    --conf "spark.kubernetes.executor.limit.cores=$EXECUTOR_CPU_LIMIT"
    --conf spark.kubernetes.executor.volumes.hostPath.bench.mount.path=/mnt/bench
    --conf spark.kubernetes.executor.volumes.hostPath.bench.options.path=/mnt/bench
    --conf spark.kubernetes.executor.volumes.hostPath.bench.options.type=Directory
    --conf spark.kubernetes.driver.volumes.hostPath.bench.mount.path=/mnt/bench
    --conf spark.kubernetes.driver.volumes.hostPath.bench.options.path=/mnt/bench
    --conf spark.kubernetes.driver.volumes.hostPath.bench.options.type=Directory
  )
  if [ -n "$NODE" ]; then
    K8S_SPARK_CONF_ARGS+=(--conf "spark.kubernetes.node.selector.kubernetes.io/hostname=$NODE")
  fi
else
  STANDALONE_SPARK_CONF_ARGS=(
    # Both the private tunnel and the public DNS route terminate at the
    # Standalone REST submission server on 6066.
    --conf spark.master.rest.enabled=true
    --conf spark.standalone.submit.waitAppCompletion=true
    --conf spark.ui.reverseProxy=true
    --conf "spark.ui.reverseProxyUrl=$STANDALONE_UI_REVERSE_PROXY_URL"
    --conf "spark.cores.max=$STANDALONE_MAX_CORES"
    # The driver-only worker advertises heterojvm_driver only. Each of the
    # three executor workers advertises one heterojvm_executor address, which
    # gives one executor per executor worker and prevents co-location.
    --conf spark.driver.resource.heterojvm_driver.amount=1
    --conf spark.executor.resource.heterojvm_executor.amount=1
  )
fi

# --- Pre-submit observability: baseline SCC stats -----------------------------
# Job #2+ is where the persistent SCC pays off, so record the cache state BEFORE
# this run's JVMs touch it. Best-effort; the g1 pool / scc-off arms no-op here.
if [ "$CLUSTER_MANAGER" = "standalone" ]; then
  mkdir -p "$LOCAL_RUN_DIR"
  capture_scc_printstats "$LOCAL_RUN_DIR/scc-before.txt"
fi

if [ -n "$JITSERVER_CPU_UNIT" ]; then
  JITSERVER_CPU_BEFORE="$(jitserver_cgroup_snapshot)"
fi

# --- Spark Submit ---
# submit->terminal wall-clock (includes JVM boot + JIT/AOT warmup, which
# duration_ms excludes) — the metric where the SCC/AOT benefit actually lives.
WALLCLOCK_START_EPOCH=$(date +%s)
# Millisecond submit/terminal stamps for the phase decomposition (GNU date; else python3).
epoch_ms() {
  local t; t="$(date +%s%3N)"
  case "$t" in
    *[!0-9]*) python3 -c 'import time; print(int(time.time() * 1000))' ;;
    *) echo "$t" ;;
  esac
}
SUBMIT_EPOCH_MS="$(epoch_ms)"
EXIT_CODE=0
SUBMIT_CAPTURE="$(mktemp -t spark-submit-output.XXXXXX)"
if "$SPARK_HOME"/bin/spark-submit \
  --master "$SUBMIT_MASTER" \
  --deploy-mode cluster \
  --name "$RUN_ID" \
  --class "$MAIN_CLASS" \
  "${COMMON_SPARK_CONF_ARGS[@]+"${COMMON_SPARK_CONF_ARGS[@]}"}" \
  "${JMX_SPARK_CONF_ARGS[@]+"${JMX_SPARK_CONF_ARGS[@]}"}" \
  "${MIXED_SPARK_CONF_ARGS[@]+"${MIXED_SPARK_CONF_ARGS[@]}"}" \
  "${STANDALONE_MIX_SPARK_CONF_ARGS[@]+"${STANDALONE_MIX_SPARK_CONF_ARGS[@]}"}" \
  "${K8S_SPARK_CONF_ARGS[@]+"${K8S_SPARK_CONF_ARGS[@]}"}" \
  "${STANDALONE_SPARK_CONF_ARGS[@]+"${STANDALONE_SPARK_CONF_ARGS[@]}"}" \
  "${MEMORY_OVERHEAD_SPARK_CONF_ARGS[@]+"${MEMORY_OVERHEAD_SPARK_CONF_ARGS[@]}"}" \
  "${S3_SECRET_ARGS[@]+"${S3_SECRET_ARGS[@]}"}" \
  "${HADOOP_SPARK_CONF_ARGS[@]+"${HADOOP_SPARK_CONF_ARGS[@]}"}" \
  "${SQL_SPARK_CONF_ARGS[@]+"${SQL_SPARK_CONF_ARGS[@]}"}" \
  "${USER_SPARK_CONF_ARGS[@]+"${USER_SPARK_CONF_ARGS[@]}"}" \
  "${JITKERNEL_SPARK_CONF_ARGS[@]+"${JITKERNEL_SPARK_CONF_ARGS[@]}"}" \
  "$WORKLOAD_JAR_URI" \
  "$QUERY" \
  "$SCALE" \
  "$DATA_LOCATION" 2>&1 | tee "$SUBMIT_CAPTURE"; then
  EXIT_CODE=0
else
  EXIT_CODE=$?
fi

# Driver pod status (spark-submit may return 0 even on driver failure). RUN_ID is already
# the normalized lowercase-hyphen form Spark uses for the spark-app-name label.
DRIVER_POD=""
APP_ID=""
if [ "$CLUSTER_MANAGER" = "kubernetes" ]; then
  DRIVER_POD=$(kubectl get pods -n spark --sort-by=.metadata.creationTimestamp --no-headers -l "spark-app-name=$RUN_ID" 2>/dev/null | grep driver || true)
  DRIVER_POD=$(echo "$DRIVER_POD" | awk '{print $1}' | tail -1)
  if [ -n "$DRIVER_POD" ]; then
    POD_STATUS=$(kubectl get pod "$DRIVER_POD" -n spark -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)
    if [ -n "$POD_STATUS" ] && [ "$POD_STATUS" != "0" ]; then EXIT_CODE="$POD_STATUS"; fi
    APP_ID=$(kubectl get pod "$DRIVER_POD" -n spark -o jsonpath='{.metadata.labels.spark-app-selector}' 2>/dev/null || true)
  fi
fi
if [ -z "$APP_ID" ]; then
  APP_ID=$(sed -n 's/.*with application ID \(spark-[[:alnum:]]*\).*/\1/p' "$SUBMIT_CAPTURE" | tail -1)
fi

# Standalone's REST client returns as soon as the Driver reaches RUNNING, even
# when spark.standalone.submit.waitAppCompletion=true. Poll the REST status
# endpoint ourselves so a successful runner result always means FINISHED.
SUBMISSION_ID=""
if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "$EXIT_CODE" = "0" ]; then
  SUBMISSION_ID=$(sed -n 's/.*"submissionId" : "\(driver-[^"]*\)".*/\1/p' "$SUBMIT_CAPTURE" | tail -1)
  if [ -z "$SUBMISSION_ID" ]; then
    echo "ERROR: Standalone REST submission did not return a driver submission ID." >&2
    EXIT_CODE=1
  else
    STATUS_MASTER="${SUBMIT_MASTER#spark://}"
    STATUS_URL="http://$STATUS_MASTER/v1/submissions/status/$SUBMISSION_ID"
    STATUS_CURL_ARGS=(-fsS --max-time 30)
    if [ "$STANDALONE_LOCAL_TUNNEL" = "true" ]; then
      STATUS_CURL_ARGS+=(--resolve "$STANDALONE_LOCAL_TUNNEL_HOST:6066:$STANDALONE_LOCAL_TUNNEL_ADDRESS")
    fi
    elapsed_ms=0
    DRIVER_STATE=""
    APP_ACTIVE_EMITTED="false"
    case "$STANDALONE_POOL" in
      g1) MASTER_SERVICE="spark-standalone-master-g1" ;;
      openj9) MASTER_SERVICE="spark-standalone-master-mix" ;;
    esac
    while [ "$elapsed_ms" -le $((STANDALONE_STATUS_TIMEOUT_SECONDS * 1000)) ]; do
      if ! STATUS_JSON=$(curl "${STATUS_CURL_ARGS[@]}" "$STATUS_URL"); then
        echo "ERROR: failed to query Standalone submission status for $SUBMISSION_ID" >&2
        EXIT_CODE=1
        break
      fi
      DRIVER_STATE=$(jq -r '.driverState // empty' <<<"$STATUS_JSON")
      case "$DRIVER_STATE" in
        FINISHED) break ;;
        FAILED|KILLED|ERROR)
          echo "ERROR: Standalone driver $SUBMISSION_ID reached terminal state $DRIVER_STATE" >&2
          EXIT_CODE=1
          break
          ;;
        RUNNING|WAITING|SUBMITTED|RELAUNCHING)
          # The Python Standalone supervisor needs the application ID while
          # executors and driver logs are still live. Emit this additive,
          # machine-readable record once; the historical RESULT record stays
          # terminal-only for compatibility.
          if [ "$APP_ACTIVE_EMITTED" = "false" ]; then
            MASTER_JSON=$(fetch_master_json)
            ACTIVE_APP_ID=$(jq -r --arg name "$RUN_ID" '[.activeapps[]? | select(.name == $name) | .id] | last // empty' <<<"$MASTER_JSON" 2>/dev/null || true)
            if [ -n "$ACTIVE_APP_ID" ]; then
              APP_ID="$ACTIVE_APP_ID"
              APP_ACTIVE_EMITTED="true"
              echo "APP_ACTIVE appid=$APP_ID runid=$RUN_ID pool=$STANDALONE_POOL submissionid=$SUBMISSION_ID"
            fi
          fi
          sleep "$STANDALONE_STATUS_POLL_SECONDS"
          elapsed_ms=$((elapsed_ms + STANDALONE_STATUS_POLL_MS))
          ;;
        *)
          echo "ERROR: unexpected Standalone driver state '${DRIVER_STATE:-<empty>}' for $SUBMISSION_ID" >&2
          EXIT_CODE=1
          break
          ;;
      esac
    done
    if [ "$DRIVER_STATE" != "FINISHED" ] && [ "$EXIT_CODE" = "0" ]; then
      echo "ERROR: timed out waiting for Standalone driver $SUBMISSION_ID after ${STANDALONE_STATUS_TIMEOUT_SECONDS}s" >&2
      EXIT_CODE=1
    fi
    if [ "$DRIVER_STATE" = "FINISHED" ]; then
      MASTER_JSON=$(fetch_master_json)
      APP_ID=$(jq -r --arg name "$RUN_ID" '[(.activeapps[]?, .completedapps[]?) | select(.name == $name) | .id] | last // empty' <<<"$MASTER_JSON")
      [ -n "$APP_ID" ] || {
        echo "ERROR: driver $SUBMISSION_ID finished but Master JSON has no matching application '$RUN_ID'." >&2
        EXIT_CODE=1
      }
    fi
  fi
fi

# submit->terminal wall-clock: driver is now terminal (k8s pod check / standalone
# FINISHED poll both done above). Captured for ALL arms (the flat G1 control needs
# it too). Includes JVM boot + warmup, unlike the query-only duration_ms.
WALLCLOCK_SECONDS=$(( $(date +%s) - WALLCLOCK_START_EPOCH ))
TERMINAL_EPOCH_MS="$(epoch_ms)"
if [ -n "$JITSERVER_CPU_UNIT" ]; then
  JITSERVER_CPU_AFTER="$(jitserver_cgroup_snapshot)"
fi
echo "WALLCLOCK_SECONDS=$WALLCLOCK_SECONDS runid=$RUN_ID pool=${STANDALONE_POOL:-k8s}"

# --- Persist driver stdout/stderr off the ephemeral worker emptyDir ----------
# Standalone cluster mode runs the driver as a DriverWrapper inside a driver-
# worker pod; its stdout (including QUERY_RESULT timing lines) lands ONLY at
#   /tmp/spark-work/<work-dir>/<submissionId>/stdout
# in that pod's emptyDir, which is wiped on pod restart. Copy it to the shared
# NFS log PVC (/var/spark-logs, durable + same export in every pod) as soon as
# the driver is terminal, keyed by RUN_ID+timestamp, and pull a local copy under
# research-related/logs/ for offline parsing. Best-effort: never changes EXIT_CODE.
# ponytail: race window = driver terminal -> this exec (seconds). A pod restart in
# that gap loses it; continuous streaming would close it but is overkill here.
# ssh mode: the work dir is on the driver node's disk, so the local copy is enough.
if [ "$CLUSTER_MANAGER" = "standalone" ] && [ -n "$SUBMISSION_ID" ] && [ "${DRY_RUN:-0}" != "1" ] \
   && [ "$STANDALONE_COLLECT" = "ssh" ]; then
  ssh_persist_driver_stdout
elif [ "$CLUSTER_MANAGER" = "standalone" ] && [ -n "$SUBMISSION_ID" ] \
     && [ "${DRY_RUN:-0}" != "1" ]; then
  case "$STANDALONE_POOL" in
    g1) DRIVER_NODE="worker1" ;;
    openj9) DRIVER_NODE="worker2" ;;
    *) DRIVER_NODE="" ;;
  esac
  DRIVER_STDOUT_REL="driver-stdout/$RUN_ID-$TIMESTAMP"
  DRIVER_WORKER_POD=""
  [ -n "$DRIVER_NODE" ] && DRIVER_WORKER_POD=$(kubectl get pods -n "$NAMESPACE" \
    -l "research-role=spark-standalone-driver-worker,research-node=$DRIVER_NODE" \
    --field-selector=status.phase=Running --no-headers 2>/dev/null | awk '{print $1}' | head -1)
  if [ -n "$DRIVER_WORKER_POD" ]; then
    if kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c '
        sub="$1"; dest="$2"
        src=$(ls -d /tmp/spark-work/*/"$sub" 2>/dev/null | head -1)
        [ -n "$src" ] || { echo "NO_DRIVER_DIR src=/tmp/spark-work/*/$sub" >&2; exit 3; }
        # Shared parent must be writable by BOTH driver uids (0 on worker1, 185 on worker2).
        # 1777 (sticky, world-writable like /tmp): any uid creates its own subdir, none clobbers others.
        # chmod may fail for the non-root run if root already owns it -> non-fatal as long as dir is writable.
        mkdir -p "/var/spark-logs/driver-stdout" 2>/dev/null || true
        chmod 1777 "/var/spark-logs/driver-stdout" 2>/dev/null || true
        mkdir -p "/var/spark-logs/$dest"
        cp "$src/stdout" "/var/spark-logs/$dest/stdout" 2>/dev/null || true
        cp "$src/stderr" "/var/spark-logs/$dest/stderr" 2>/dev/null || true
        ls -l "/var/spark-logs/$dest"
      ' _ "$SUBMISSION_ID" "$DRIVER_STDOUT_REL"; then
      echo "DRIVER_STDOUT saved=/var/spark-logs/$DRIVER_STDOUT_REL pod=$DRIVER_WORKER_POD submissionid=$SUBMISSION_ID"
      mkdir -p "$LOCAL_RUN_DIR"
      kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c 'cat "/var/spark-logs/'"$DRIVER_STDOUT_REL"'/stdout" 2>/dev/null' > "$LOCAL_RUN_DIR/stdout" 2>/dev/null || true
      kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c 'cat "/var/spark-logs/'"$DRIVER_STDOUT_REL"'/stderr" 2>/dev/null' > "$LOCAL_RUN_DIR/stderr" 2>/dev/null || true
      echo "DRIVER_STDOUT local=$LOCAL_RUN_DIR"
    else
      echo "WARN: could not persist driver stdout for $SUBMISSION_ID from $DRIVER_WORKER_POD (dir may already be recycled)" >&2
    fi
  else
    echo "WARN: no Running driver-worker pod for pool $STANDALONE_POOL (node ${DRIVER_NODE:-?}); driver stdout for $SUBMISSION_ID not persisted" >&2
  fi
fi

# --- Per-run observability: GC logs + provenance + SCC after-stats ------------
# Durable evidence next to the driver stdout, same RUN_ID+timestamp folder. GC
# logs live on the shared PVC under gc-logs-raw/<executor-pod>/; every worker pod
# sees all executors' dirs, so we glob this run by its $TIMESTAMP-$RUN_ID prefix.
# All best-effort: nothing here changes EXIT_CODE.
if [ "$CLUSTER_MANAGER" = "standalone" ] && [ "${DRY_RUN:-0}" != "1" ]; then
  mkdir -p "$LOCAL_RUN_DIR/gc"
  case "$STANDALONE_POOL" in g1) OBS_NODE="worker1" ;; openj9) OBS_NODE="worker2" ;; *) OBS_NODE="" ;; esac
  # ssh mode: no worker pods, so skip the pod-based cgroup read and GC pull below.
  if [ "$STANDALONE_COLLECT" = "ssh" ]; then OBS_NODE=""; fi
  OBS_CGROUP_MEM=""; [ -n "$OBS_NODE" ] && OBS_CGROUP_MEM="$(read_cgroup_mem_limit "$OBS_NODE")"

  {
    echo "run_id=$RUN_ID"
    echo "run_folder=$RUN_FOLDER_NAME"
    echo "timestamp=$TIMESTAMP"
    echo "exit_code=$EXIT_CODE"
    echo "wallclock_s=$WALLCLOCK_SECONDS"
    echo "submit_epoch_ms=$SUBMIT_EPOCH_MS"
    echo "terminal_epoch_ms=$TERMINAL_EPOCH_MS"
    echo "warmup_verbose=$WARMUP_VERBOSE"
    echo "arm=${ARM_LABEL:-$GC}"
    echo "gc_policy=$GC"
    echo "jvm_family=$JVM_FAMILY"
    echo "pool=$STANDALONE_POOL"
    echo "benchmark=$BENCHMARK"
    echo "query=$QUERY"
    echo "scale=$SCALE"
    echo "heap=$HEAP"
    echo "overhead=${OVERHEAD:-<default>}"
    echo "driver_mem=$DRIVER_MEMORY"
    echo "driver_overhead=${DRIVER_OVERHEAD:-<default>}"
    echo "cores=$CORES"
    echo "instances=$INSTANCES"
    echo "tag=$TAG"
    echo "submit_master=$SUBMIT_MASTER"
    echo "image=worker-pool-managed"
    echo "cgroup_mem_limit_bytes=${OBS_CGROUP_MEM:-n/a}"
    echo "scc_enabled=$OPENJ9_SCC_ENABLED"
    echo "scc_name=${OPENJ9_SCC_NAME:-n/a}"
    echo "scc_sccmx=$OPENJ9_SCCMX"
    echo "jitserver=$JITSERVER_ENABLED"
    echo "driver_extraJavaOptions=$DRIVER_JAVA_OPTS"
    echo "executor_extraJavaOptions=$EXECUTOR_JAVA_OPTS"
    if [ "$STANDALONE_COLLECT" = "ssh" ]; then
      echo "collect_mode=ssh"
      echo "ssh_nodes=$STANDALONE_SSH_NODES"
      echo "ssh_driver_node=$STANDALONE_SSH_DRIVER_NODE"
      echo "work_dir=$STANDALONE_WORK_DIR"
    fi
    if [ -n "$JITSERVER_CPU_UNIT" ]; then
      jitserver_cpu_provenance
    fi
  } > "$LOCAL_RUN_DIR/provenance.txt"

  capture_scc_printstats "$LOCAL_RUN_DIR/scc-after.txt"

  # Pull this run's GC logs from the shared PVC (any worker pod sees every dir).
  OBS_POD=""; [ -n "$OBS_NODE" ] && OBS_POD="$(find_executor_worker_pod "$OBS_NODE")"
  if [ -n "$OBS_POD" ]; then
    OBS_GC_LIST="$(kubectl exec -n "$NAMESPACE" "$OBS_POD" -- sh -c 'ls /var/spark-logs/gc-logs-raw/*/'"$TIMESTAMP-$RUN_ID"'-*.log 2>/dev/null' 2>/dev/null || true)"
    for src in $OBS_GC_LIST; do
      base="$(basename "$src")"
      kubectl exec -n "$NAMESPACE" "$OBS_POD" -- sh -c 'cat "'"$src"'"' > "$LOCAL_RUN_DIR/gc/$base" 2>/dev/null || true
    done
    # JIT/AOT compile-trace vlogs (opt-in, OpenJ9). Named "*-jit.<date>.<time>.<pid>"
    # so they do NOT match the gc "*.log" glob above -> pulled into a separate jit/.
    if [ "$WARMUP_VERBOSE" = "true" ] && { [ "$JVM_FAMILY" = "openj9" ] || [ "$JVM_FAMILY" = "hotspot" ]; }; then
      mkdir -p "$LOCAL_RUN_DIR/jit"
      OBS_JIT_LIST="$(kubectl exec -n "$NAMESPACE" "$OBS_POD" -- sh -c 'ls /var/spark-logs/gc-logs-raw/*/'"$TIMESTAMP-$RUN_ID"'-jit.* 2>/dev/null' 2>/dev/null || true)"
      for src in $OBS_JIT_LIST; do
        base="$(basename "$src")"
        kubectl exec -n "$NAMESPACE" "$OBS_POD" -- sh -c 'cat "'"$src"'"' > "$LOCAL_RUN_DIR/jit/$base" 2>/dev/null || true
      done
    fi
  fi
  # ssh mode: /logs is node-local, so pull this run's GC logs (and opt-in JIT traces)
  # from every node into the same gc/ and jit/ folders.
  if [ "$STANDALONE_COLLECT" = "ssh" ]; then
    ssh_pull_logs "$TIMESTAMP-$RUN_ID-*.log" "$LOCAL_RUN_DIR/gc"
    if [ "$WARMUP_VERBOSE" = "true" ] \
       && { [ "$JVM_FAMILY" = "openj9" ] || [ "$JVM_FAMILY" = "hotspot" ]; }; then
      mkdir -p "$LOCAL_RUN_DIR/jit"
      ssh_pull_logs "$TIMESTAMP-$RUN_ID-jit.*" "$LOCAL_RUN_DIR/jit"
    fi
  fi

  # Mirror provenance + SCC + GC logs into the durable PVC run folder.
  if [ -n "${DRIVER_WORKER_POD:-}" ] && [ -n "${DRIVER_STDOUT_REL:-}" ]; then
    kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c '
        dest="/var/spark-logs/'"$DRIVER_STDOUT_REL"'"; mkdir -p "$dest/gc" "$dest/jit"
        cp /var/spark-logs/gc-logs-raw/*/'"$TIMESTAMP-$RUN_ID"'-*.log "$dest/gc/" 2>/dev/null || true
        cp /var/spark-logs/gc-logs-raw/*/'"$TIMESTAMP-$RUN_ID"'-jit.* "$dest/jit/" 2>/dev/null || true' \
      >/dev/null 2>&1 || true
    kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c 'cat > "/var/spark-logs/'"$DRIVER_STDOUT_REL"'/provenance.txt"' < "$LOCAL_RUN_DIR/provenance.txt" >/dev/null 2>&1 || true
    [ -s "$LOCAL_RUN_DIR/scc-before.txt" ] && kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c 'cat > "/var/spark-logs/'"$DRIVER_STDOUT_REL"'/scc-before.txt"' < "$LOCAL_RUN_DIR/scc-before.txt" >/dev/null 2>&1 || true
    [ -s "$LOCAL_RUN_DIR/scc-after.txt" ]  && kubectl exec -n "$NAMESPACE" "$DRIVER_WORKER_POD" -- sh -c 'cat > "/var/spark-logs/'"$DRIVER_STDOUT_REL"'/scc-after.txt"'  < "$LOCAL_RUN_DIR/scc-after.txt"  >/dev/null 2>&1 || true
  fi
  OBS_GC_COUNT=$({ ls "$LOCAL_RUN_DIR/gc" 2>/dev/null || true; } | wc -l | tr -d ' ')
  OBS_JIT_COUNT=$({ ls "$LOCAL_RUN_DIR/jit" 2>/dev/null || true; } | wc -l | tr -d ' ')
  echo "OBSERVABILITY local=$LOCAL_RUN_DIR gc_logs=$OBS_GC_COUNT jit_vlogs=$OBS_JIT_COUNT provenance=yes scc_before=$([ -s "$LOCAL_RUN_DIR/scc-before.txt" ] && echo yes || echo no) scc_after=$([ -s "$LOCAL_RUN_DIR/scc-after.txt" ] && echo yes || echo no)"
  echo "OBSERVABILITY parse: $ARTIFACT_ROOT/execution/parse-run.sh \"$LOCAL_RUN_DIR\""
fi
rm -f "$SUBMIT_CAPTURE"

echo ""
echo "=============================================="
echo "Screening Complete: $RUN_ID"
echo "  Exit Code:    $EXIT_CODE"
if [ -n "$SUBMISSION_ID" ]; then echo "  Submission ID: $SUBMISSION_ID"; fi
echo "  App ID:       ${APP_ID:-unknown}"
echo "  Timestamp:    $TIMESTAMP"
echo "=============================================="

# Machine-parseable (key=value so the parser is robust to field changes).
echo "RESULT exit=$EXIT_CODE appid=${APP_ID:-unknown} runid=$RUN_ID gc=$GC bench=$BENCHMARK query=$QUERY ts=$TIMESTAMP"
