#!/usr/bin/env bash
# No 'set -e': this parser must be tolerant of empty greps (a field simply
# missing from a log is normal -> print n/a, never abort). pipefail/nounset stay.
set -uo pipefail
# =============================================================================
# parse-run.sh <run-folder>  ->  internal-state card (stdout + <folder>/card.txt)
#
# Reads a persisted per-run folder (produced by run-screening.sh) and emits a
# compact "internal-state card" so the fragmentation verdict is READ from the
# JVM's own logs, not guessed. Pure text processing (grep/awk); tolerant of
# missing fields (prints n/a, never crashes). Runs offline from the folder.
#
# Expected folder contents (any may be absent):
#   stdout stderr provenance.txt scc-before.txt scc-after.txt gc/<*.log>
#
# G1 (HotSpot -Xlog) and OpenJ9 (-Xverbosegclog) logs are auto-detected and
# parsed with the matching field set.
# =============================================================================

DIR="${1:?usage: parse-run.sh <run-folder>}"
[ -d "$DIR" ] || { echo "ERROR: not a directory: $DIR" >&2; exit 2; }

PROV="$DIR/provenance.txt"
STDOUT="$DIR/stdout"
SCC_AFTER="$DIR/scc-after.txt"
SCC_BEFORE="$DIR/scc-before.txt"

# All gc logs concatenated (may be several executors + driver). Empty if none.
GC_GLOB=("$DIR"/gc/*.log)
GC_FILES=()
for f in "${GC_GLOB[@]}"; do [ -f "$f" ] && GC_FILES+=("$f"); done

# --- tiny helpers ------------------------------------------------------------
pget() { # pget <key>  -> value from provenance.txt, or empty
  [ -f "$PROV" ] || return 0
  sed -n "s/^$1=//p" "$PROV" | head -1
}
na() { [ -n "${1:-}" ] && printf '%s' "$1" || printf 'n/a'; }

gc_cat() { [ "${#GC_FILES[@]}" -gt 0 ] && cat "${GC_FILES[@]}" 2>/dev/null || true; }
gc_grep_c() { # count matches across all gc logs (fixed string)
  local n; n="$(gc_cat | grep -c -- "$1" 2>/dev/null || true)"; echo "${n:-0}"
}
gc_egrep_c() { local n; n="$(gc_cat | grep -Ec -- "$1" 2>/dev/null || true)"; echo "${n:-0}"; }

# --- detect JVM family -------------------------------------------------------
FAMILY="$(pget jvm_family)"
if [ -z "$FAMILY" ]; then
  if gc_cat | grep -q '<gc-op\|verbosegc\|<exclusive-end'; then FAMILY="openj9"
  elif gc_cat | grep -q '\[gc'; then FAMILY="hotspot"
  else FAMILY="unknown"; fi
fi

# --- common fields -----------------------------------------------------------
ARM="$(pget arm)"
TAG="$(pget tag)"
POOL="$(pget pool)"
CGROUP="$(pget cgroup_mem_limit_bytes)"
EXIT_CODE="$(pget exit_code)"
GC_FLAGS="$(pget executor_extraJavaOptions)"

# duration_ms fallback from the workload stdout marker
DURATION_MS="n/a"
if [ -f "$STDOUT" ]; then
  DURATION_MS="$(sed -n 's/.*QUERY_RESULT:[^:]*:[^:]*:[0-9]*:\([0-9.]*\):.*/\1/p' "$STDOUT" | head -1)"
  [ -n "$DURATION_MS" ] || DURATION_MS="n/a"
fi

# wallclock_s: submit->terminal seconds captured by run-screening.sh (includes JVM
# boot + JIT/AOT warmup, which duration_ms excludes) — the metric where the SCC/AOT
# benefit lives. Fall back to the gc-log time-of-day span for pre-existing runs.
WALLCLOCK_S="$(pget wallclock_s)"
if [ -z "$WALLCLOCK_S" ]; then
  WALLCLOCK_S="$(gc_cat | grep -oE 'T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]+' \
    | awk -F'[T:]' 'NR==1{f=$2*3600+$3*60+$4} {l=$2*3600+$3*60+$4} END{if(NR>0){d=l-f; if(d<0)d+=86400; printf "%.1f", d}}')"
fi
[ -n "$WALLCLOCK_S" ] || WALLCLOCK_S="n/a"

# JIT/AOT warmup evidence (OpenJ9, --warmup-verbose runs only). Count compile-trace
# RESULT lines in jit/*: "+ (AOT load)" = method loaded from AOT/SCC (warm benefit);
# every other "+ (...)" (cold / AOT warm / warm / hot / scorching) = fresh compile
# (cold cost). Leading "!" lines are failed loads -> excluded (they retry as a compile).
AOT_LOAD_COUNT="n/a"; JIT_COMPILE_COUNT="n/a"; AOT_LOAD_PCT="n/a"
JIT_GLOB=("$DIR"/jit/*)
JIT_FILES=()
for f in "${JIT_GLOB[@]}"; do [ -f "$f" ] && JIT_FILES+=("$f"); done
if [ "${#JIT_FILES[@]}" -gt 0 ]; then
  _total="$(grep -hcE '^\+ \(' "${JIT_FILES[@]}" 2>/dev/null | awk '{s+=$1}END{print s+0}')"
  _loads="$(grep -hcE '^\+ \(AOT load\)' "${JIT_FILES[@]}" 2>/dev/null | awk '{s+=$1}END{print s+0}')"
  AOT_LOAD_COUNT="$_loads"
  JIT_COMPILE_COUNT="$(( _total - _loads ))"
  if [ "$_total" -gt 0 ]; then
    AOT_LOAD_PCT="$(awk -v l="$_loads" -v t="$_total" 'BEGIN{printf "%.1f", 100*l/t}')"
  fi
fi

if [ "${#GC_FILES[@]}" -eq 0 ]; then
  echo "WARN: no gc/*.log in $DIR — GC fields will be n/a" >&2
fi

# --- G1 (HotSpot) parsing ----------------------------------------------------
parse_g1() {
  local region full mixed young_all young total to_space evac conc
  region="$(gc_cat | sed -n 's/.*Heap Region Size: \([0-9]*[KMG]\).*/\1/p' | head -1)"
  full="$(gc_grep_c 'Pause Full')"
  mixed="$(gc_grep_c 'Pause Young (Mixed)')"
  young_all="$(gc_grep_c 'Pause Young')"
  young=$(( young_all - mixed )); [ "$young" -lt 0 ] && young=0
  total=$(( young + mixed + full ))
  to_space="$(gc_grep_c 'to-space exhausted')"
  evac="$(gc_egrep_c 'Evacuation [Ff]ailure')"
  conc="$(gc_grep_c 'Concurrent Mark Cycle')"
  [ "$conc" = "0" ] && conc="$(gc_grep_c 'Concurrent Cycle')"

  # humongous regions high-water: max over all "Humongous regions: X->Y" values.
  local humongous
  humongous="$(gc_cat | grep -oE 'Humongous regions: [0-9]+->[0-9]+' \
    | grep -oE '[0-9]+' | awk 'BEGIN{m=0}{if($1>m)m=$1}END{if(NR>0)print m; else print "n/a"}')"

  # peak heap used: max post-collection used from "NNN[KMG]->NNN[KMG](NNN[KMG])".
  local peak
  peak="$(gc_cat | grep -oE '[0-9]+[BKMG]->[0-9]+[BKMG]\([0-9]+[BKMG]\)' \
    | sed -E 's/.*->([0-9]+)([BKMG])\(.*/\1 \2/' \
    | awk '{u=$1; if($2=="K")u*=1024; else if($2=="M")u*=1048576; else if($2=="G")u*=1073741824; if(u>m)m=u} END{if(NR>0)printf "%d", m; else print "n/a"}')"

  # STW pause total/max from trailing "NNN.NNNms" on Pause lines.
  local pauses
  pauses="$(gc_cat | grep -E 'Pause (Young|Full|Remark|Cleanup)' \
    | grep -oE '[0-9]+\.[0-9]+ms$' | grep -oE '[0-9]+\.[0-9]+' \
    | awk 'BEGIN{s=0;mx=0}{s+=$1; if($1>mx)mx=$1}END{if(NR>0)printf "%.1f %.1f", s, mx; else print "n/a n/a"}')"

  # JIT-tax evidence (HotSpot, --warmup-verbose runs only). compile_count: one
  # "[jit,compilation]" line per compiled method in jit/*.hotspot (all captured
  # JVMs). compile_seconds: CITime "Total compilation time : N s" — printed at JVM
  # exit to stdout, so this is the DRIVER's total (executor CITime lands in the
  # standalone worker stdout, not captured here); n/a without --warmup-verbose.
  local hs_compile_count="n/a" hs_compile_seconds="n/a"
  if [ "${#JIT_FILES[@]}" -gt 0 ]; then
    hs_compile_count="$(grep -hc '\[jit,compilation\]' "${JIT_FILES[@]}" 2>/dev/null | awk '{s+=$1}END{print s+0}')"
  fi
  if [ -f "$STDOUT" ]; then
    hs_compile_seconds="$(sed -n 's/.*Total compilation time[[:space:]]*:[[:space:]]*\([0-9.]*\)[[:space:]]*s.*/\1/p' "$STDOUT" | head -1)"
    [ -n "$hs_compile_seconds" ] || hs_compile_seconds="n/a"
  fi

  cat <<EOF
hotspot_compile_count=$hs_compile_count
hotspot_compile_seconds=$hs_compile_seconds
g1_region_size=$(na "$region")
gc_count_total=$total
young_count=$young
mixed_count=$mixed
full_gc_count=$full
humongous_regions_highwater=$humongous
to_space_exhausted_count=$to_space
evacuation_failure_count=$evac
concurrent_cycles=$conc
stw_pause_total_ms=${pauses% *}
stw_pause_max_ms=${pauses#* }
peak_heap_used_bytes=$peak
EOF
}

# --- OpenJ9 (gencon/balanced) parsing ---------------------------------------
parse_openj9() {
  local total global compact af reasons
  total="$(gc_grep_c '<cycle-start')"
  # gencon: type="global"; balanced full global: type="global garbage collect".
  # "global mark phase" is the concurrent mark cycle, not a STW global GC.
  global="$(gc_egrep_c '<cycle-start[^>]*type="global( garbage collect)?"')"
  compact="$(gc_egrep_c '<gc-op[^>]*type="compact"')"
  af="$(gc_grep_c '<af-start')"
  reasons="$(gc_cat | grep -oE '<compact-info[^>]*reason="[^"]*"' \
    | sed -E 's/.*reason="([^"]*)".*/\1/' | sort -u | paste -sd';' -)"
  [ -n "$reasons" ] || reasons="n/a"

  # free_pct_min: tightest whole-heap free moment from <mem-info percent="N">.
  local freepct
  freepct="$(gc_cat | grep -oE '<mem-info[^>]*percent="[0-9]+"' \
    | grep -oE 'percent="[0-9]+"' | grep -oE '[0-9]+' \
    | awk 'BEGIN{m=101}{if($1<m)m=$1}END{if(m<=100)print m; else print "n/a"}')"

  # peak heap used: max (total-free) over <mem-info free="F" total="T">.
  local peak
  peak="$(gc_cat | grep -oE '<mem-info[^>]*free="[0-9]+" total="[0-9]+"' \
    | sed -E 's/.*free="([0-9]+)" total="([0-9]+)".*/\1 \2/' \
    | awk '{u=$2-$1; if(u>m)m=u} END{if(NR>0)printf "%d", m; else print "n/a"}')"

  # STW pauses from <exclusive-end durationms="X">.
  local pauses
  pauses="$(gc_cat | grep -oE '<exclusive-end[^>]*durationms="[0-9.]+"' \
    | grep -oE 'durationms="[0-9.]+"' | grep -oE '[0-9.]+' \
    | awk 'BEGIN{s=0;mx=0}{s+=$1; if($1>mx)mx=$1}END{if(NR>0)printf "%.1f %.1f", s, mx; else print "n/a n/a"}')"

  cat <<EOF
gc_count_total=$total
global_gc_count=$global
compact_count=$compact
compact_reasons=$reasons
free_pct_min=$freepct
allocation_failure_gc_count=$af
stw_pause_total_ms=${pauses% *}
stw_pause_max_ms=${pauses#* }
peak_heap_used_bytes=$peak
EOF
}

# --- SCC after-stats (OpenJ9) ------------------------------------------------
scc_field() { # scc_field <file> <printStats label prefix>
  [ -f "$1" ] || { echo "n/a"; return; }
  sed -n "s/^[[:space:]]*$2[[:space:]]*= *//p" "$1" | head -1 | tr -d ' ' | sed 's/^$/n\/a/'
}
scc_full() { # "Cache is NN% full"
  [ -f "$1" ] || { echo "n/a"; return; }
  sed -n 's/.*Cache is \([0-9]*%\) full.*/\1/p' "$1" | head -1 | sed 's/^$/n\/a/'
}

# --- emit card ---------------------------------------------------------------
CARD="$DIR/card.txt"
{
  echo "=== internal-state card: $(basename "$DIR") ==="
  echo "arm=$(na "$ARM")"
  echo "pool=$(na "$POOL")"
  echo "jvm_family=$FAMILY"
  echo "tag=$(na "$TAG")"
  echo "exit_code=$(na "$EXIT_CODE")"
  echo "duration_ms=$(na "$DURATION_MS")"
  echo "wallclock_s=$WALLCLOCK_S"
  # AOT/JIT warmup counters: n/a for HotSpot (no app AOT) and for OpenJ9 runs
  # without --warmup-verbose (no jit/ vlog captured).
  echo "aot_load_count=$AOT_LOAD_COUNT"
  echo "jit_compile_count=$JIT_COMPILE_COUNT"
  echo "aot_load_pct=$AOT_LOAD_PCT"
  echo "cgroup_mem_limit=$(na "$CGROUP")"
  echo "gc_flags_effective=$(na "$GC_FLAGS")"
  case "$FAMILY" in
    hotspot) parse_g1 ;;
    openj9)  parse_openj9
             echo "scc_cache_size_bytes=$(scc_field "$SCC_AFTER" 'cache size')"
             echo "scc_free_bytes_after=$(scc_field "$SCC_AFTER" 'free bytes')"
             echo "scc_aot_methods_after=$(scc_field "$SCC_AFTER" '# AOT Methods')"
             echo "scc_pct_full_after=$(scc_full "$SCC_AFTER")"
             echo "scc_pct_stale_after=$(scc_field "$SCC_AFTER" '% Stale classes')"
             echo "scc_free_bytes_before=$(scc_field "$SCC_BEFORE" 'free bytes')"
             echo "scc_pct_full_before=$(scc_full "$SCC_BEFORE")"
             ;;
    *) echo "gc_parse=unavailable (no recognizable gc log)" ;;
  esac
} | tee "$CARD"
