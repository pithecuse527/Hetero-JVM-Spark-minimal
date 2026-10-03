#!/usr/bin/env bash
set -uo pipefail

# Clear runtime state between JSON batches.
#
# Clears:
# - Linux page cache on selected worker nodes. This is the relevant host-side
#   cache control for HotSpot/G1 after Spark JVMs exit.
# - OpenJ9 local SCC/AOT files under /mnt/scc on selected worker nodes.
# - JITServer in-memory state by restarting jitserver-worker1/2 deployments.
#
# HotSpot JIT/code cache is process-local; there is no persistent HotSpot JIT
# cache to delete after driver/executor JVMs exit.
#
# --mode ssh (bare-metal Standalone) reaches each node as $SSH_USER@<node> ("local"
# = this host), drops the page cache, and destroys the OpenJ9 caches under the
# node-local SCC dir (default /scc) with Semeru's -Xshareclasses:destroyAll. There
# are no JITServer deployments in ssh mode; --jitserver-restart <host>:<unit>[,...] opts in
# to restarting a systemd-managed JITServer and waiting until its port accepts
# connections. Without it the JITServer is left running.

NS="${NS:-spark}"
MODE="${MODE:-kubectl}"
SSH_USER="${SSH_USER:-root}"
SSH_OPTS="${SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=10}"
SCC_DIR="${SCC_DIR:-/scc}"
SEMERU_JAVA="${SEMERU_JAVA:-/opt/java/semeru/bin/java}"
JITSERVER_RESTART=""
JITSERVER_PORT="${JITSERVER_PORT:-38400}"
JITSERVER_WAIT_SECONDS="${JITSERVER_WAIT_SECONDS:-120}"
# Address probed for the port when the JITServer host is "local".
JITSERVER_PROBE_LOCAL="${JITSERVER_PROBE_LOCAL:-127.0.0.1}"
DROP_PAGE_CACHE=1
WIPE_SCC=1
RESTART_JITSERVER=1
NODES=()

usage() {
  cat <<'EOF'
Usage:
  ./clear-runtime-caches.sh --nodes worker1,worker2 [options]

Options:
  --mode kubectl|ssh         kubectl privileged pods (default) or ssh to bare-metal nodes
  --namespace NS             Kubernetes namespace (default: spark)
  --nodes a,b                Worker node names (kubectl) or ssh hosts (ssh) to clear
  --scc-dir DIR              ssh mode: SCC cacheDir to destroy (default: /scc)
  --jitserver-restart H:U[,H:U...]  ssh mode: systemctl restart unit U on each host H
                             ("local" = here); all are restarted, then each is probed
                             on its own node at 127.0.0.1,
                             then wait for the JITServer port (default: no restart)
  --jitserver-port N         port to wait for after the restart (default: 38400)
  --jitserver-wait S         seconds to wait for the port (default: 120)
  --skip-page-cache          Do not drop Linux page cache
  --skip-scc                 Do not wipe /mnt/scc (ssh mode: the --scc-dir caches)
  --skip-jitserver-restart   Do not restart jitserver-worker1/2 deployments
  -h, --help                 Show help
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="${2:?--mode needs kubectl|ssh}"; shift ;;
    --namespace|-n) NS="${2:?--namespace needs a namespace}"; shift ;;
    --scc-dir) SCC_DIR="${2:?--scc-dir needs a directory}"; shift ;;
    --jitserver-restart) JITSERVER_RESTART="${2:?--jitserver-restart needs host:unit}"; shift ;;
    --jitserver-port) JITSERVER_PORT="${2:?--jitserver-port needs a port}"; shift ;;
    --jitserver-wait) JITSERVER_WAIT_SECONDS="${2:?--jitserver-wait needs seconds}"; shift ;;
    --nodes)
      IFS=',' read -r -a NODES <<< "${2:?--nodes needs a comma-separated node list}"
      shift
      ;;
    --skip-page-cache) DROP_PAGE_CACHE=0 ;;
    --skip-scc) WIPE_SCC=0 ;;
    --skip-jitserver-restart) RESTART_JITSERVER=0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown arg '$1'" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

case "$MODE" in
  kubectl) command -v kubectl >/dev/null || { echo "ERROR: kubectl not on PATH" >&2; exit 2; } ;;
  ssh)
    command -v ssh >/dev/null || { echo "ERROR: ssh not on PATH" >&2; exit 2; }
    case "$SCC_DIR" in
      /?*) ;;
      *) echo "ERROR: --scc-dir must be an absolute path, got '$SCC_DIR'" >&2; exit 2 ;;
    esac
    ;;
  *) echo "ERROR: --mode must be kubectl|ssh, got '$MODE'" >&2; exit 2 ;;
esac
JITSERVER_ENTRIES=()
if [ -n "$JITSERVER_RESTART" ]; then
  [ "$MODE" = "ssh" ] || { echo "ERROR: --jitserver-restart needs --mode ssh" >&2; exit 2; }
  IFS=',' read -r -a JITSERVER_ENTRIES <<< "$JITSERVER_RESTART"
  for entry in "${JITSERVER_ENTRIES[@]}"; do
    case "$entry" in
      ?*:?*) ;;
      *) echo "ERROR: --jitserver-restart entries must be host:unit, got '$entry'" >&2; exit 2 ;;
    esac
    case "${entry%%:*}${entry#*:}" in
      *[!A-Za-z0-9_.@-]*)
        echo "ERROR: invalid --jitserver-restart entry '$entry'" >&2; exit 2 ;;
    esac
  done
  case "$JITSERVER_PORT$JITSERVER_WAIT_SECONDS" in
    *[!0-9]*) echo "ERROR: --jitserver-port/--jitserver-wait must be integers" >&2; exit 2 ;;
  esac
fi
[ "${#NODES[@]}" -gt 0 ] || { echo "ERROR: --nodes is required" >&2; exit 2; }

clear_node() {
  local node="$1"
  local pod="runtime-cache-clear-${node}-$(date +%s)-$RANDOM"
  local status=0

  echo "  node=$node page_cache=$DROP_PAGE_CACHE scc=$WIPE_SCC"
  kubectl -n "$NS" delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true

  cat <<EOF | kubectl -n "$NS" apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: $pod
spec:
  restartPolicy: Never
  nodeName: $node
  hostPID: true
  tolerations:
  - key: dataplatform
    operator: Exists
    effect: NoSchedule
  containers:
  - name: clear
    image: busybox:1.36
    securityContext:
      privileged: true
    command:
    - sh
    - -c
    - |
      set -u
      echo "NODE=$node"
      if [ "$DROP_PAGE_CACHE" = "1" ]; then
        sync
        echo 3 > /proc/sys/vm/drop_caches
        echo "PAGE_CACHE_DROPPED"
      else
        echo "PAGE_CACHE_SKIPPED"
      fi
      if [ "$WIPE_SCC" = "1" ]; then
        echo "SCC_BEFORE"
        find /mnt/scc -maxdepth 1 -mindepth 1 -print 2>/dev/null | sort || true
        rm -rf /mnt/scc/* 2>/dev/null || true
        echo "SCC_AFTER"
        find /mnt/scc -maxdepth 1 -mindepth 1 -print 2>/dev/null | sort || true
        echo "SCC_WIPED"
      else
        echo "SCC_SKIPPED"
      fi
    volumeMounts:
    - name: scc
      mountPath: /mnt/scc
  volumes:
  - name: scc
    hostPath:
      path: /mnt/scc
      type: DirectoryOrCreate
EOF

  if ! kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$pod" --timeout=120s >/dev/null 2>&1; then
    status=1
    echo "  cache-clear pod failed or timed out: $pod" >&2
    kubectl -n "$NS" get pod "$pod" -o wide >&2 || true
    kubectl -n "$NS" describe pod "$pod" >&2 || true
  fi
  kubectl -n "$NS" logs "$pod" 2>/dev/null | sed 's/^/    /' || true
  kubectl -n "$NS" delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  return "$status"
}

# ssh mode: same steps and markers as the pod script above, run on the host itself.
clear_node_ssh() {
  local node="$1" status=0
  local script='
    set -u
    echo "NODE=$1"
    if [ "$2" = "1" ]; then
      sync
      echo 3 > /proc/sys/vm/drop_caches && echo "PAGE_CACHE_DROPPED" || exit 1
    else
      echo "PAGE_CACHE_SKIPPED"
    fi
    if [ "$3" = "1" ]; then
      echo "SCC_BEFORE"
      find "$4" -maxdepth 1 -mindepth 1 -print 2>/dev/null | sort || true
      if [ -d "$4" ]; then
        "$5" -Xshareclasses:cacheDir="$4",destroyAll 2>&1 || true
      fi
      echo "SCC_AFTER"
      left="$(find "$4" -maxdepth 1 -mindepth 1 ! -name lost+found -print 2>/dev/null | sort)"
      [ -z "$left" ] || printf "%s\n" "$left"
      [ -z "$left" ] || { echo "SCC_RESIDUE_LEFT"; exit 1; }
      echo "SCC_WIPED"
    else
      echo "SCC_SKIPPED"
    fi'

  echo "  node=$node page_cache=$DROP_PAGE_CACHE scc=$WIPE_SCC scc_dir=$SCC_DIR"
  if [ "$node" = "local" ]; then
    sh -s -- "$node" "$DROP_PAGE_CACHE" "$WIPE_SCC" "$SCC_DIR" "$SEMERU_JAVA" <<<"$script" \
      2>&1 | sed 's/^/    /' || status=1
  else
    # shellcheck disable=SC2086  # SSH_OPTS is a word list by design.
    ssh $SSH_OPTS "$SSH_USER@$node" "sh -s -- $(printf '%q ' "$node" "$DROP_PAGE_CACHE" \
      "$WIPE_SCC" "$SCC_DIR" "$SEMERU_JAVA")" <<<"$script" 2>&1 | sed 's/^/    /' || status=1
  fi
  [ "$status" -eq 0 ] || echo "  cache clear failed on $node" >&2
  return "$status"
}

# ssh mode: restart systemd JITServer units, then wait until each accepts connections on
# 127.0.0.1 of its own node (the clients connect to their node-local server).
port_open() {
  if command -v nc >/dev/null 2>&1; then
    nc -z -w 2 "$1" "$2" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/$1/$2") >/dev/null 2>&1
  fi
}

restart_jitserver_ssh() {
  [ -n "$JITSERVER_RESTART" ] || { echo "  JITServer restart not requested (ssh mode)"; return 0; }
  local entry host unit waited status=0
  local probe_remote='p="$1"
if command -v nc >/dev/null 2>&1; then nc -z -w 2 127.0.0.1 "$p" >/dev/null 2>&1
else bash -c "exec 3<>/dev/tcp/127.0.0.1/$p" >/dev/null 2>&1; fi'
  for entry in "${JITSERVER_ENTRIES[@]}"; do
    host="${entry%%:*}"; unit="${entry#*:}"
    echo "  restarting JITServer unit=$unit host=$host port=$JITSERVER_PORT"
    if [ "$host" = "local" ]; then
      systemctl restart "$unit" || { echo "  systemctl restart $unit failed" >&2; status=1; }
    else
      # shellcheck disable=SC2086  # SSH_OPTS is a word list by design.
      ssh $SSH_OPTS "$SSH_USER@$host" "systemctl restart $(printf '%q' "$unit")" || {
        echo "  systemctl restart $unit on $host failed" >&2
        status=1
      }
    fi
  done
  # A restart returns before the old listener is gone; give it a moment to drop.
  sleep 1
  for entry in "${JITSERVER_ENTRIES[@]}"; do
    host="${entry%%:*}"; waited=0
    while true; do
      if [ "$host" = "local" ]; then
        port_open "$JITSERVER_PROBE_LOCAL" "$JITSERVER_PORT" && break
      else
        # shellcheck disable=SC2086
        ssh $SSH_OPTS "$SSH_USER@$host" "sh -s -- $JITSERVER_PORT" <<<"$probe_remote" \
          && break
      fi
      if [ "$waited" -ge "$JITSERVER_WAIT_SECONDS" ]; then
        echo "  JITServer on $host not accepting on 127.0.0.1:$JITSERVER_PORT after" \
          "${waited}s" >&2
        status=1
        continue 2
      fi
      sleep 1
      waited=$((waited + 1))
    done
    echo "  JITServer ready on $host (127.0.0.1:$JITSERVER_PORT) after ~$((waited + 1))s"
  done
  return "$status"
}

restart_jitservers() {
  [ "$RESTART_JITSERVER" = "1" ] || { echo "  JITServer restart skipped"; return 0; }
  if [ "$MODE" = "ssh" ]; then
    restart_jitserver_ssh
    return
  fi

  # Restart only the JITServer(s) for the --nodes being cleared, so clearing one
  # node between jobs does NOT kill an in-flight job on another node's JITServer.
  local status=0 restarted=0 deploy node
  local deploys=()
  for node in "${NODES[@]}"; do
    [ -n "$node" ] || continue
    deploys+=("jitserver-$node")
  done

  for deploy in "${deploys[@]}"; do
    if kubectl -n "$NS" get deployment "$deploy" >/dev/null 2>&1; then
      echo "  restarting deployment/$deploy"
      if kubectl -n "$NS" rollout restart "deployment/$deploy" >/dev/null; then
        restarted=$((restarted + 1))
      else
        status=1
      fi
    else
      echo "  deployment/$deploy not found; skipping"
    fi
  done

  for deploy in "${deploys[@]}"; do
    if kubectl -n "$NS" get deployment "$deploy" >/dev/null 2>&1; then
      kubectl -n "$NS" rollout status "deployment/$deploy" --timeout=180s || status=1
    fi
  done

  echo "  JITServer deployments restarted: $restarted"
  return "$status"
}

echo "=============================================="
echo " Clearing runtime caches"
echo "=============================================="
if [ "$MODE" = "ssh" ]; then
  echo "  mode:      ssh  scc_dir=$SCC_DIR"
else
  echo "  namespace: $NS"
fi
echo "  nodes:     ${NODES[*]}"

overall=0
for node in "${NODES[@]}"; do
  [ -n "$node" ] || continue
  if [ "$MODE" = "ssh" ]; then
    clear_node_ssh "$node" || overall=1
  else
    clear_node "$node" || overall=1
  fi
done
restart_jitservers || overall=1

if [ "$overall" -eq 0 ]; then
  echo "  runtime cache clear: OK"
else
  echo "  runtime cache clear: FAIL" >&2
fi
exit "$overall"
