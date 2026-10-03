#!/usr/bin/env bash
set -euo pipefail
# Phase 2b: convert the UDFBench CSVs (staged by udfbench-data-fetch.sh) to
# Parquet on WORKER1, scale by scale (small -> medium -> large). Each scale is a
# SEPARATE worker1-pinned k8s Spark job (cloned from datagen-tpch pattern) that
# writes to the writable /mnt/bench hostPath - it never runs on the measured g1
# standalone pool. CSVs for a scale are extracted from the tar just before, and
# deleted right after that scale's parquet lands, to cap peak disk.
#
# Final layout: /mnt/bench/udfbench/{small,medium,large}/parquet/<table>/
#               /mnt/bench/udfbench/files/   (shared external files)

NS="${SPARK_K8S_NAMESPACE:-spark}"
POD="${UDFBENCH_HELPER_POD:-udfbench-helper}"
IMAGE="${SPARK_K8S_IMAGE:-apache/spark:4.1.2}"
SERVER="${SPARK_MASTER_URL:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')}"
SPARK_HOME="${SPARK_HOME:?SPARK_HOME must point at a spark-submit client}"

ROOT=/mnt/bench/udfbench
WORK="$ROOT/_work"
TAR="$WORK/udfbench-dataset.tar.gz"
PYSRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/udfbench_csv2parquet.py"
PYDST=/var/spark-logs/udfbench/udfbench_csv2parquet.py     # on spark-logs-pvc (driver-visible)

SCALES=("${@:-small medium large}"); SCALES=(${SCALES[*]})
MIN_FREE_GB="${UDFBENCH_MIN_FREE_GB:-15}"
LARGE_MIN_FREE_GB="${UDFBENCH_LARGE_MIN_FREE_GB:-25}"

k(){ kubectl -n "$NS" "$@"; }
# retry: kubectl exec to the apiserver occasionally hits a transient i/o timeout;
# without this, set -e aborts the whole run on a blip. 3 tries, 5s apart.
inpod(){ local i; for i in 1 2 3; do if k exec "$POD" -- sh -lc "$1"; then return 0; fi; sleep 5; done; return 1; }

k get pod "$POD" >/dev/null 2>&1 || { echo "ERROR: helper pod $POD missing; run udfbench-data-fetch.sh first." >&2; exit 1; }
k wait --for=condition=Ready "pod/$POD" --timeout=120s >/dev/null
inpod "test -f '$TAR'" || { echo "ERROR: tar $TAR missing; run udfbench-data-fetch.sh first." >&2; exit 1; }

free_gb(){ inpod "df -BG --output=avail /mnt/bench | tail -1 | tr -dc 0-9"; }
require_free(){ local min="$1" a; a=$(free_gb); echo "free /mnt/bench: ${a}GB (need >= ${min}GB)"
  (( a >= min )) || { echo "ABORT: free ${a}GB below ${min}GB; not proceeding (disk safety)." >&2; exit 3; }; }

# stage the converter onto the pvc so the k8s driver can read it as local://
echo "staging converter -> $POD:$PYDST"
inpod "mkdir -p $(dirname "$PYDST")"
k exec -i "$POD" -- sh -c "cat > $PYDST" < "$PYSRC"

# worker1 carries a dataplatform:NoSchedule taint; Spark k8s has no toleration
# conf, so tolerate it via pod templates (same as datagen-tpch-spark-experiment.sh).
POD_TEMPLATE="$(mktemp -t udfbench-c2p-pod.XXXXXX.yaml)"
trap 'rm -f "$POD_TEMPLATE"' EXIT
cat > "$POD_TEMPLATE" <<'YAML'
apiVersion: v1
kind: Pod
spec:
  tolerations:
    - { key: dataplatform, operator: Exists, effect: NoSchedule }
YAML

submit_scale(){
  local scale="$1" run_id="udfbench-c2p-${1}-$(date +%Y%m%d-%H%M%S)"
  "$SPARK_HOME/bin/spark-submit" \
    --master "k8s://$SERVER" --deploy-mode cluster --name "$run_id" \
    --conf spark.kubernetes.driver.podTemplateFile="$POD_TEMPLATE" \
    --conf spark.kubernetes.executor.podTemplateFile="$POD_TEMPLATE" \
    --conf spark.kubernetes.namespace="$NS" \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
    --conf spark.kubernetes.node.selector.kubernetes.io/hostname=worker1 \
    --conf spark.kubernetes.driver.label.run-id="$run_id" \
    --conf spark.kubernetes.executor.label.run-id="$run_id" \
    --conf spark.executor.instances=2 --conf spark.executor.cores=4 \
    --conf spark.executor.memory=4g --conf spark.executor.memoryOverhead=1g \
    --conf spark.driver.memory=2g --conf spark.driver.memoryOverhead=512m \
    --conf spark.kubernetes.driver.request.cores=250m --conf spark.kubernetes.driver.limit.cores=1 \
    --conf spark.kubernetes.executor.request.cores=500m --conf spark.kubernetes.executor.limit.cores=4 \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError" \
    --conf spark.dynamicAllocation.enabled=false --conf spark.speculation=false \
    --conf spark.sql.parquet.compression.codec=snappy \
    --conf spark.network.timeout=3600s --conf spark.executor.heartbeatInterval=60s \
    --conf spark.kubernetes.submit.waitAppCompletion=true \
    --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:/var/spark-logs \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=/var/spark-logs \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.path=/var/spark-logs \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.mount.readOnly=false \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-logs-pvc.options.claimName=spark-logs-pvc \
    --conf spark.kubernetes.driver.volumes.hostPath.bench-data.mount.path=/mnt/bench \
    --conf spark.kubernetes.driver.volumes.hostPath.bench-data.options.path=/mnt/bench \
    --conf spark.kubernetes.driver.volumes.hostPath.bench-data.options.type=DirectoryOrCreate \
    --conf spark.kubernetes.executor.volumes.hostPath.bench-data.mount.path=/mnt/bench \
    --conf spark.kubernetes.executor.volumes.hostPath.bench-data.options.path=/mnt/bench \
    --conf spark.kubernetes.executor.volumes.hostPath.bench-data.options.type=DirectoryOrCreate \
    "local://$PYDST" \
    --input "file://$WORK/csvs/$scale" --output "file://$ROOT/$scale" --scale "$scale"
}

for scale in "${SCALES[@]}"; do
  echo ""; echo "================ scale: $scale ================"
  if inpod "test -f '$ROOT/$scale/_rowcounts.txt'"; then
    echo "$scale already converted (_rowcounts.txt present); skipping."
    inpod "cat '$ROOT/$scale/_rowcounts.txt'"; continue
  fi
  [ "$scale" = "large" ] && require_free "$LARGE_MIN_FREE_GB" || require_free "$MIN_FREE_GB"

  # extract just this scale's CSVs (idempotent)
  if inpod "test -d '$WORK/csvs/$scale' && [ \$(ls -A '$WORK/csvs/$scale' 2>/dev/null | wc -l) -gt 0 ]"; then
    echo "csvs for $scale already extracted."
  else
    echo "extracting dataset/csvs/$scale ..."
    inpod "mkdir -p '$WORK/csvs/$scale' && tar -xzf '$TAR' -C '$WORK/csvs/$scale' --strip-components=3 'dataset/csvs/$scale'"
  fi
  inpod "echo 'csv du:'; du -sh '$WORK/csvs/$scale'; ls '$WORK/csvs/$scale'" || true

  echo "submitting conversion job for $scale ..."
  submit_scale "$scale"

  inpod "test -f '$ROOT/$scale/_rowcounts.txt'" || { echo "ERROR: $scale conversion produced no _rowcounts.txt" >&2; exit 1; }
  echo "-- $scale row counts --"; inpod "cat '$ROOT/$scale/_rowcounts.txt'"
  echo "deleting extracted csvs for $scale to reclaim disk ..."
  inpod "rm -rf '$WORK/csvs/$scale'"
  echo "free after $scale: $(free_gb)GB"
done

echo ""; echo "================ final layout ================"
inpod "for s in small medium large; do [ -d '$ROOT/'\$s ] || continue; echo \"== \$s ==\"; du -sh '$ROOT/'\$s; ls -1 '$ROOT/'\$s/parquet 2>/dev/null; done; echo '== files =='; du -sh '$ROOT/files'; echo '== df =='; df -h /mnt/bench | tail -1" || true
echo "done. Tar retained at $TAR (delete manually to reclaim ~4.7GB)."
