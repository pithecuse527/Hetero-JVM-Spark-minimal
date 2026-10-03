#!/usr/bin/env bash
set -euo pipefail
# Phase 2a: download the UDFBench dataset tar onto WORKER1 node-local /mnt/bench
# and stage the shared external files. Idempotent. Uses a worker1-pinned helper
# pod (writable /mnt/bench hostPath + spark-logs-pvc) - the same writable-hostPath
# mechanism the tpch datagen uses. Leaves the tar at $WORK/udfbench-dataset.tar.gz
# for udfbench-csv2parquet.sh to extract from, scale by scale.
#
# Does NOT touch worker2 (user rsyncs /mnt/bench later) and does NOT touch the
# measured g1 standalone pool.

NS="${SPARK_K8S_NAMESPACE:-spark}"
POD="${UDFBENCH_HELPER_POD:-udfbench-helper}"
IMAGE="${SPARK_K8S_IMAGE:-apache/spark:4.1.2}"
URL="https://zenodo.org/records/14260428/files/udfbench-dataset.tar.gz"
TAR_SIZE=4730596132            # from Zenodo content-length; used for skip check
ROOT=/mnt/bench/udfbench
WORK="$ROOT/_work"
TAR="$WORK/udfbench-dataset.tar.gz"
MIN_FREE_GB="${UDFBENCH_MIN_FREE_GB:-50}"   # startup gate

k(){ kubectl -n "$NS" "$@"; }
# retry: kubectl exec occasionally hits a transient apiserver i/o timeout; without
# this, set -e aborts the run on a blip. 3 tries, 5s apart.
inpod(){ local i; for i in 1 2 3; do if k exec "$POD" -- sh -lc "$1"; then return 0; fi; sleep 5; done; return 1; }

ensure_pod(){
  if k get pod "$POD" >/dev/null 2>&1; then
    k wait --for=condition=Ready "pod/$POD" --timeout=120s >/dev/null
    return
  fi
  echo "creating helper pod $POD on worker1..."
  k apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels: { app: udfbench-helper }
spec:
  restartPolicy: Never
  nodeSelector: { kubernetes.io/hostname: worker1 }
  tolerations:
    - { key: dataplatform, operator: Exists, effect: NoSchedule }
  containers:
    - name: helper
      image: $IMAGE
      imagePullPolicy: IfNotPresent
      command: ["sleep","infinity"]
      resources:
        requests: { cpu: "250m", memory: "512Mi" }
        limits:   { cpu: "2",    memory: "2Gi" }
      volumeMounts:
        - { name: bench, mountPath: /mnt/bench }
        - { name: logs,  mountPath: /var/spark-logs }
  volumes:
    - name: bench
      hostPath: { path: /mnt/bench, type: DirectoryOrCreate }
    - name: logs
      persistentVolumeClaim: { claimName: spark-logs-pvc }
YAML
  k wait --for=condition=Ready "pod/$POD" --timeout=180s >/dev/null
  echo "helper pod ready."
}

check_disk(){
  local avail; avail=$(inpod "df -BG --output=avail /mnt/bench | tail -1 | tr -dc 0-9")
  echo "free /mnt/bench: ${avail}GB (min ${MIN_FREE_GB}GB)"
  if (( avail < MIN_FREE_GB )); then
    echo "HOLD: free space ${avail}GB below gate ${MIN_FREE_GB}GB; not downloading." >&2
    exit 3
  fi
}

ensure_pod
check_disk
inpod "mkdir -p '$WORK' '$ROOT/files'"

# --- download (skip if tar already the right size) ---
# Run the download DETACHED inside the pod (setsid) so a slow ~4.7GB Zenodo pull
# (~1-2MB/s, tens of minutes) survives a dropped kubectl-exec connection; then
# poll. ponytail: no HTTP-range resume - a restart re-downloads from scratch;
# add Range/resume only if Zenodo drops mid-transfer often enough to matter.
have=$(inpod "test -f '$TAR' && stat -c%s '$TAR' 2>/dev/null || echo 0")
if [ "$have" = "$TAR_SIZE" ]; then
  echo "tar already present and correct size ($have bytes); skipping download."
else
  echo "starting detached download of tar ($TAR_SIZE bytes) into $TAR ..."
  inpod "mkdir -p /var/spark-logs/udfbench; cat > /var/spark-logs/udfbench/dl.py <<'PY'
import urllib.request, shutil, os
u='$URL'; d='$TAR'
with urllib.request.urlopen(u) as r, open(d,'wb') as f:
    shutil.copyfileobj(r, f, 4*1024*1024)
print('done', os.path.getsize(d))
PY
setsid nohup python3 /var/spark-logs/udfbench/dl.py > /var/spark-logs/udfbench/dl.log 2>&1 < /dev/null &
echo detached"
  echo "polling until download completes (Ctrl-C safe; it keeps running in-pod)..."
  until inpod "s=\$(stat -c%s '$TAR' 2>/dev/null||echo 0); grep -q done /var/spark-logs/udfbench/dl.log 2>/dev/null || [ \"\$s\" -ge $TAR_SIZE ]"; do
    printf '  %s bytes\r' "$(inpod "stat -c%s '$TAR' 2>/dev/null||echo 0")"; sleep 15
  done
  got=$(inpod "stat -c%s '$TAR'")
  [ "$got" = "$TAR_SIZE" ] || { echo "ERROR: tar size $got != expected $TAR_SIZE (see /var/spark-logs/udfbench/dl.log)" >&2; exit 1; }
  echo "download ok ($got bytes)."
fi

# --- stage shared external files (single copy, not per-scale) ---
if inpod "test -d '$ROOT/files' && [ \$(ls -A '$ROOT/files' | wc -l) -gt 0 ]"; then
  echo "external files already staged at $ROOT/files; skipping."
else
  echo "extracting external files -> $ROOT/files ..."
  inpod "tar -xzf '$TAR' -C '$ROOT/files' --strip-components=2 dataset/externalfiles"
fi

echo "== fetch summary =="
inpod "echo 'tar:'; ls -la '$TAR'; echo 'external files (count / du):'; find '$ROOT/files' -type f | wc -l; du -sh '$ROOT/files'; echo 'scales inside tar:'; tar -tzf '$TAR' 'dataset/csvs' 2>/dev/null | sed -n 's#dataset/csvs/\([^/]*\)/.*#\1#p' | sort -u"
echo "fetch done. Next: udfbench-csv2parquet.sh"
