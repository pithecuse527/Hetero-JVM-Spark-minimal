#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Generate TPC-H data directly onto worker-local /mnt/bench hostPath storage.

Default target:
  worker1 and worker2, sequentially, writing file:///mnt/bench/tpch-scale-20

Usage:
  ./datagen-tpch-spark-experiment.sh [--scale 20] [--nodes worker1,worker2]
                                    [--output /mnt/bench/tpch-scale-20]
                                    [--format parquet] [--jar local:///...]
                                    [--overwrite true|false] [--dry-run]

Compatibility aliases:
  ./datagen-tpch-spark-experiment.sh 20
  ./datagen-tpch-spark-experiment.sh 20 /mnt/bench/tpch-scale-20 parquet

Important:
  The default jar is local:///var/spark-logs/jars/tpcds-datagen-5.jar.
  That file must exist inside Spark driver/executor pods through the
  spark-logs-pvc mount before this script can run without S3.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${SPARK_K8S_NAMESPACE:-spark}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')}"
SPARK_HOME="${SPARK_HOME:?SPARK_HOME must point at the Spark submit client}"

SCALE_FACTOR="20"
OUTPUT_PATH="/mnt/bench/tpch-scale-20"
FORMAT="parquet"
TARGET_NODES="worker1,worker2"
TPCH_JAR="${TPCH_DATAGEN_JAR:-local:///var/spark-logs/jars/tpcds-datagen-5.jar}"
IMAGE="${SPARK_K8S_IMAGE:-apache/spark:4.1.2}"
OVERWRITE="true"
DRY_RUN="false"

# Conservative defaults: worker1/worker2 already host persistent Standalone pods.
NUM_PARTITIONS="${TPCH_DATAGEN_NUM_PARTITIONS:-24}"
EXECUTOR_CORES="${TPCH_DATAGEN_EXECUTOR_CORES:-2}"
EXECUTOR_MEMORY="${TPCH_DATAGEN_EXECUTOR_MEMORY:-2g}"
EXECUTOR_INSTANCES="${TPCH_DATAGEN_EXECUTOR_INSTANCES:-2}"
MEMORY_OVERHEAD="${TPCH_DATAGEN_MEMORY_OVERHEAD:-512m}"
SHUFFLE_PARTITIONS="${TPCH_DATAGEN_SHUFFLE_PARTITIONS:-48}"
DRIVER_CORES="${TPCH_DATAGEN_DRIVER_CORES:-1}"
DRIVER_MEMORY="${TPCH_DATAGEN_DRIVER_MEMORY:-1g}"
DRIVER_OVERHEAD="${TPCH_DATAGEN_DRIVER_OVERHEAD:-512m}"
DRIVER_REQUEST_CORES="${TPCH_DATAGEN_DRIVER_REQUEST_CORES:-250m}"
DRIVER_LIMIT_CORES="${TPCH_DATAGEN_DRIVER_LIMIT_CORES:-1}"
EXECUTOR_REQUEST_CORES="${TPCH_DATAGEN_EXECUTOR_REQUEST_CORES:-500m}"
EXECUTOR_LIMIT_CORES="${TPCH_DATAGEN_EXECUTOR_LIMIT_CORES:-2}"
MAX_RECORDS_PER_FILE="${TPCH_DATAGEN_MAX_RECORDS_PER_FILE:-3000000}"

if [[ $# -gt 0 && "$1" != --* ]]; then
  SCALE_FACTOR="${1:-$SCALE_FACTOR}"
  OUTPUT_PATH="${2:-/mnt/bench/tpch-scale-$SCALE_FACTOR}"
  FORMAT="${3:-$FORMAT}"
  shift $#
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale) SCALE_FACTOR="${2:?}"; OUTPUT_PATH="/mnt/bench/tpch-scale-${2:?}"; shift 2 ;;
    --nodes) TARGET_NODES="${2:?}"; shift 2 ;;
    --node) TARGET_NODES="${2:?}"; shift 2 ;;
    --output) OUTPUT_PATH="${2:?}"; shift 2 ;;
    --format) FORMAT="${2:?}"; shift 2 ;;
    --jar) TPCH_JAR="${2:?}"; shift 2 ;;
    --image) IMAGE="${2:?}"; shift 2 ;;
    --partitions) NUM_PARTITIONS="${2:?}"; shift 2 ;;
    --executor-cores) EXECUTOR_CORES="${2:?}"; shift 2 ;;
    --executor-memory) EXECUTOR_MEMORY="${2:?}"; shift 2 ;;
    --executor-instances) EXECUTOR_INSTANCES="${2:?}"; shift 2 ;;
    --memory-overhead) MEMORY_OVERHEAD="${2:?}"; shift 2 ;;
    --shuffle-partitions) SHUFFLE_PARTITIONS="${2:?}"; shift 2 ;;
    --overwrite) OVERWRITE="${2:?}"; shift 2 ;;
    --dry-run) DRY_RUN="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$OUTPUT_PATH" in
  file://*) OUTPUT_URI="$OUTPUT_PATH"; OUTPUT_LOCAL_PATH="${OUTPUT_PATH#file://}" ;;
  /*) OUTPUT_URI="file://$OUTPUT_PATH"; OUTPUT_LOCAL_PATH="$OUTPUT_PATH" ;;
  *) echo "ERROR: --output must be an absolute path or file:// URI, got: $OUTPUT_PATH" >&2; exit 2 ;;
esac

DRIVER_POD_TEMPLATE="$(mktemp -t tpch-datagen-driver.XXXXXX.yaml)"
EXECUTOR_POD_TEMPLATE="$(mktemp -t tpch-datagen-executor.XXXXXX.yaml)"
cleanup() {
  rm -f "$DRIVER_POD_TEMPLATE" "$EXECUTOR_POD_TEMPLATE"
}
trap cleanup EXIT

write_pod_template() {
  local path="$1" container_name="$2"
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
}

observer_pod_for_node() {
  local node="$1"
  kubectl -n "$NAMESPACE" get pods \
    --field-selector "spec.nodeName=$node,status.phase=Running" \
    -l app.kubernetes.io/component=worker \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

check_local_jar() {
  case "$TPCH_JAR" in
    local:///*|file:///*)
      local jar_path="${TPCH_JAR#local://}"
      jar_path="${jar_path#file://}"
      local pod
      pod="$(kubectl -n "$NAMESPACE" get pods \
        -l app.kubernetes.io/component=worker \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
      if [[ -z "$pod" ]]; then
        echo "ERROR: no running Spark worker pod found to check $jar_path" >&2
        exit 1
      fi
      if ! kubectl -n "$NAMESPACE" exec "$pod" -- sh -lc "test -s '$jar_path'" >/dev/null 2>&1; then
        echo "ERROR: datagen jar not found in pods: $jar_path" >&2
        echo "Place the jar on spark-logs-pvc, for example at /var/spark-logs/jars/tpcds-datagen-5.jar," >&2
        echo "or rerun with --jar pointing at a pod-visible jar URI." >&2
        exit 1
      fi
      ;;
  esac
}

validate_node_output() {
  local node="$1" pod
  pod="$(observer_pod_for_node "$node")"
  if [[ -z "$pod" ]]; then
    echo "WARN: no running worker pod on $node to validate $OUTPUT_LOCAL_PATH" >&2
    return 0
  fi
  echo "Validation view from $node via pod/$pod:"
  kubectl -n "$NAMESPACE" exec "$pod" -- sh -lc "
    set -e
    test -d '$OUTPUT_LOCAL_PATH'
    du -sh '$OUTPUT_LOCAL_PATH'
    find '$OUTPUT_LOCAL_PATH' -mindepth 1 -maxdepth 1 -type d | sort
    for d in '$OUTPUT_LOCAL_PATH'/*; do
      [ -d \"\$d\" ] || continue
      printf '%s files=%s %s\n' \"\$(du -sh \"\$d\" | cut -f1)\" \"\$(find \"\$d\" -type f | wc -l | tr -d ' ')\" \"\$d\"
    done | sort -k3
  "
}

run_for_node() {
  local node="$1"
  local timestamp run_id
  local -a overwrite_flag
  timestamp="$(date +%Y%m%d-%H%M%S)"
  run_id="tpch-datagen-sf${SCALE_FACTOR}-${node}-${timestamp}"
  overwrite_flag=()
  if [[ "$OVERWRITE" == "true" ]]; then
    overwrite_flag=(--overwrite)
  fi

  echo ""
  echo "============================================"
  echo "TPC-H datagen on $node"
  echo "============================================"
  echo "Namespace:          $NAMESPACE"
  echo "Spark master:       k8s://$SPARK_MASTER_URL"
  echo "Image:              $IMAGE"
  echo "Jar:                $TPCH_JAR"
  echo "Output:             $OUTPUT_URI"
  echo "Scale factor:       $SCALE_FACTOR"
  echo "Format:             $FORMAT"
  echo "Partitions:         $NUM_PARTITIONS"
  echo "Executors:          $EXECUTOR_INSTANCES x $EXECUTOR_CORES cores, $EXECUTOR_MEMORY + $MEMORY_OVERHEAD overhead"
  echo "K8s CPU requests:   driver=$DRIVER_REQUEST_CORES executor=$EXECUTOR_REQUEST_CORES"
  echo "Run ID:             $run_id"

  local cmd=(
    "$SPARK_HOME/bin/spark-submit"
    --master "k8s://$SPARK_MASTER_URL"
    --deploy-mode cluster
    --name "$run_id"
    --class org.apache.spark.sql.execution.revisedbenchmark.TPCHDatagen
    --driver-cores "$DRIVER_CORES"
    --driver-memory "$DRIVER_MEMORY"
    --executor-cores "$EXECUTOR_CORES"
    --executor-memory "$EXECUTOR_MEMORY"
    --conf "spark.kubernetes.namespace=$NAMESPACE"
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
    --conf "spark.kubernetes.container.image=$IMAGE"
    --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent
    --conf "spark.kubernetes.driver.podTemplateFile=$DRIVER_POD_TEMPLATE"
    --conf "spark.kubernetes.executor.podTemplateFile=$EXECUTOR_POD_TEMPLATE"
    --conf "spark.kubernetes.node.selector.kubernetes.io/hostname=$node"
    --conf "spark.kubernetes.driver.label.run-id=$run_id"
    --conf "spark.kubernetes.executor.label.run-id=$run_id"
    --conf "spark.kubernetes.driver.request.cores=$DRIVER_REQUEST_CORES"
    --conf "spark.kubernetes.driver.limit.cores=$DRIVER_LIMIT_CORES"
    --conf "spark.kubernetes.executor.request.cores=$EXECUTOR_REQUEST_CORES"
    --conf "spark.kubernetes.executor.limit.cores=$EXECUTOR_LIMIT_CORES"
    --conf "spark.executor.instances=$EXECUTOR_INSTANCES"
    --conf "spark.executor.memoryOverhead=$MEMORY_OVERHEAD"
    --conf "spark.driver.memoryOverhead=$DRIVER_OVERHEAD"
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
    --conf spark.dynamicAllocation.enabled=false
    --conf spark.speculation=false
    --conf spark.sql.adaptive.enabled=false
    --conf spark.sql.adaptive.coalescePartitions.enabled=false
    --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS"
    --conf "spark.default.parallelism=$((EXECUTOR_CORES * EXECUTOR_INSTANCES))"
    --conf "spark.sql.files.maxRecordsPerFile=$MAX_RECORDS_PER_FILE"
    --conf spark.sql.parquet.compression.codec=snappy
    --conf spark.network.timeout=3600s
    --conf spark.executor.heartbeatInterval=60s
    --conf spark.rpc.askTimeout=3600s
    --conf spark.rpc.lookupTimeout=3600s
    --conf spark.kubernetes.allocation.batch.delay=10s
    --conf spark.kubernetes.executor.request.timeout=3600s
    --conf spark.eventLog.enabled=true
    --conf spark.eventLog.dir=file:/var/spark-logs
    --conf spark.kubernetes.submit.waitAppCompletion=true
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=/var/spark-logs
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=/var/spark-logs
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc
    --conf spark.kubernetes.driver.volumes.hostPath.bench-data.mount.path=/mnt/bench
    --conf spark.kubernetes.driver.volumes.hostPath.bench-data.options.path=/mnt/bench
    --conf spark.kubernetes.driver.volumes.hostPath.bench-data.options.type=DirectoryOrCreate
    --conf spark.kubernetes.executor.volumes.hostPath.bench-data.mount.path=/mnt/bench
    --conf spark.kubernetes.executor.volumes.hostPath.bench-data.options.path=/mnt/bench
    --conf spark.kubernetes.executor.volumes.hostPath.bench-data.options.type=DirectoryOrCreate
    "$TPCH_JAR"
    --output-location "$OUTPUT_URI"
    --scale-factor "$SCALE_FACTOR"
    --format "$FORMAT"
    --num-partitions "$NUM_PARTITIONS"
    --partition-tables
    --cluster-by-partition-columns
    "${overwrite_flag[@]}"
  )

  if [[ "$DRY_RUN" == "true" ]]; then
    printf 'DRY_RUN:'
    printf ' %q' "${cmd[@]}"
    printf '\n'
  else
    "${cmd[@]}"
    validate_node_output "$node"
  fi
}

write_pod_template "$DRIVER_POD_TEMPLATE" spark-kubernetes-driver
write_pod_template "$EXECUTOR_POD_TEMPLATE" spark-kubernetes-executor
if [[ "$DRY_RUN" != "true" ]]; then
  check_local_jar
fi

IFS=',' read -r -a NODE_ARRAY <<< "$TARGET_NODES"
for node in "${NODE_ARRAY[@]}"; do
  node="${node//[[:space:]]/}"
  [[ -n "$node" ]] || continue
  if ! kubectl get node "$node" >/dev/null 2>&1; then
    echo "ERROR: Kubernetes node not found: $node" >&2
    exit 1
  fi
  run_for_node "$node"
done

echo ""
echo "TPC-H datagen request complete for nodes: $TARGET_NODES"
