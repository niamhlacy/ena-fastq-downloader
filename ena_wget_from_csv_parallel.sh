#!/usr/bin/env bash
# ENA FASTQ Downloader with Strain Renaming (parallel, brief console)
# -------------------------------------------------------------------
# Console shows only start/end per strain. Full details go to logs/STRAIN.log
#
# Usage:
#   ./ena_wget_from_csv_parallel.sh mapping.csv
#
# mapping.csv format (header optional, commas or tabs OK):
#   strain,ERR
#   1D-053,ERR4013432
#
# Env vars:
#   OUTDIR=fastq    # output folder
#   JOBS=6          # parallel workers
#   WGET_OPTS=""    # extra wget flags (e.g., "--no-check-certificate")
#   LOGDIR=logs     # logs & status folder

set -euo pipefail

# ---------- Args & config ----------
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 mapping.csv" >&2
  exit 2
fi

MAPPING_FILE="$1"
OUTDIR="${OUTDIR:-fastq}"
JOBS="${JOBS:-6}"
WGET_OPTS="${WGET_OPTS:-}"
LOGDIR="${LOGDIR:-logs}"

command -v wget >/dev/null 2>&1 || { echo "Error: wget is required." >&2; exit 2; }
mkdir -p "$OUTDIR" "$LOGDIR"

ts() { date +"%Y-%m-%d %H:%M:%S"; }

# Keep dashes; only spaces/tabs → underscore; trim trailing underscores
safe_strain() {
  echo "$1" | tr ' \t' '_' | sed 's/_$//'
}

# ---------- Worker: download one strain (brief console, detailed logs) ----------
download_one() {
  local STRAIN_RAW="$1" ERR="$2"
  local STRAIN; STRAIN="$(safe_strain "$STRAIN_RAW")"

  local log="${LOGDIR}/${STRAIN}.log"
  local ok="${LOGDIR}/${STRAIN}.ok"
  local fail="${LOGDIR}/${STRAIN}.fail"
  rm -f "$ok" "$fail" "$log"

  echo "[$(ts)] Starting: ${STRAIN} (${ERR})"

  local result="FAIL"  # will be flipped to OK if all good

  {
    echo "[$(ts)] Lookup ${STRAIN} (${ERR})"
    local RESP
    if ! RESP=$(wget -qO- \
      "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=${ERR}&result=read_run&fields=run_accession,fastq_ftp&download=true"); then
      echo "[$(ts)] ERROR: API request failed for ${ERR}"
      echo "${ERR}" > "$fail"
      result="FAIL"
      exit 0
    fi

    local FASTQ_FTP
    FASTQ_FTP=$(echo "$RESP" | awk -v r="$ERR" -F '\t' 'NR>1 && $1==r {print $2}')
    if [[ -z "${FASTQ_FTP}" ]]; then
      echo "[$(ts)] WARN: No FASTQ URLs for ${ERR}"
      echo "${ERR}" > "$fail"
      result="FAIL"
      exit 0
    fi

    # Normalize separators and trim whitespace
    FASTQ_FTP=${FASTQ_FTP//;/,}
    IFS=',' read -r -a PARTS <<< "$FASTQ_FTP"
    for i in "${!PARTS[@]}"; do PARTS[$i]="${PARTS[$i]//[$'\t\r\n ']}"; done

    local BASE_WGET=(wget -nc --tries=10 --timeout=30 --continue $WGET_OPTS)
    local rc=0

    if (( ${#PARTS[@]} == 2 )); then
      local URL1="https://${PARTS[0]}"
      local URL2="https://${PARTS[1]}"
      local OUT1="${OUTDIR}/${STRAIN}_1.fastq.gz"
      local OUT2="${OUTDIR}/${STRAIN}_2.fastq.gz"

      echo "[$(ts)] Downloading PE"
      echo "[$(ts)] URL1: $URL1 -> $OUT1"
      "${BASE_WGET[@]}" "$URL1" -O "$OUT1" || rc=1
      echo "[$(ts)] URL2: $URL2 -> $OUT2"
      "${BASE_WGET[@]}" "$URL2" -O "$OUT2" || rc=1

      if [[ $rc -eq 0 && -s "$OUT1" && -s "$OUT2" ]]; then
        echo "[$(ts)] OK: ${STRAIN}"
        echo "${ERR}" > "$ok"
        result="OK"
      else
        echo "[$(ts)] FAIL: ${STRAIN} (missing/empty files)"
        echo "${ERR}" > "$fail"
        result="FAIL"
      fi

    elif (( ${#PARTS[@]} == 1 )); then
      local URL="https://${PARTS[0]}"
      local OUT="${OUTDIR}/${STRAIN}.fastq.gz"

      echo "[$(ts)] Downloading SE"
      echo "[$(ts)] URL: $URL -> $OUT"
      "${BASE_WGET[@]}" "$URL" -O "$OUT" || rc=1

      if [[ $rc -eq 0 && -s "$OUT" ]]; then
        echo "[$(ts)] OK: ${STRAIN}"
        echo "${ERR}" > "$ok"
        result="OK"
      else
        echo "[$(ts)] FAIL: ${STRAIN} (missing/empty file)"
        echo "${ERR}" > "$fail"
        result="FAIL"
      fi

    else
      echo "[$(ts)] WARN: Unexpected #files for ${ERR}: ${#PARTS[@]}  (${FASTQ_FTP})"
      echo "${ERR}" > "$fail"
      result="FAIL"
    fi
  } >> "$log" 2>&1

  if [[ "$result" == "OK" ]]; then
    echo "[$(ts)] Finished OK: ${STRAIN}"
  else
    echo "[$(ts)] Finished FAIL: ${STRAIN}  (see ${log})"
  fi
}

export -f download_one
export OUTDIR WGET_OPTS LOGDIR

echo "[INFO] Output:  $OUTDIR"
echo "[INFO] Logs:    $LOGDIR"
echo "[INFO] Jobs:    $JOBS"
echo "[INFO] Mapping: $MAPPING_FILE"
echo

# ---------- Normalize mapping → "STRAIN<TAB>ERR" ----------
TMP_FIFO="$(mktemp -u)"
mkfifo "$TMP_FIFO"
trap 'rm -f "$TMP_FIFO"' EXIT

awk -F'[,\t]' '
  NR==1 {
    hdr = (tolower($1) ~ /strain/) && (tolower($2) ~ /(err|accession)/)
    if (!hdr && NF>=2) {
      gsub(/^[ \t]+|[ \t]+$/, "", $1)
      gsub(/^[ \t]+|[ \t]+$/, "", $2)
      print $1 "\t" $2
    }
    next
  }
  NF>=2 {
    gsub(/^[ \t]+|[ \t]+$/, "", $1)
    gsub(/^[ \t]+|[ \t]+$/, "", $2)
    if ($1 != "" && $2 != "") print $1 "\t" $2
  }
' "$MAPPING_FILE" > "$TMP_FIFO" &

# ---------- Launch workers with simple concurrency control ----------
i=0
while IFS=$'\t' read -r STRAIN ERR; do
  STRAIN="${STRAIN%$'\r'}"
  ERR="${ERR%$'\r'}"
  ( download_one "$STRAIN" "$ERR" ) &
  (( i++ ))
  if (( i % JOBS == 0 )); then
    wait
  fi
done < "$TMP_FIFO"

wait

# ---------- Summary ----------
OK_COUNT=$(ls -1 "$LOGDIR"/*.ok 2>/dev/null | wc -l | tr -d ' ')
FAIL_COUNT=$(ls -1 "$LOGDIR"/*.fail 2>/dev/null | wc -l | tr -d ' ')
TOTAL=$(( OK_COUNT + FAIL_COUNT ))

echo
echo "================= SUMMARY ================="
echo "Total strains:  ${TOTAL}"
echo "Succeeded:      ${OK_COUNT}"
echo "Failed:         ${FAIL_COUNT}"
if (( FAIL_COUNT > 0 )); then
  echo
  echo "Failed strains (see ${LOGDIR}/*.log for details):"
  for f in "$LOGDIR"/*.fail; do
    [[ -e "$f" ]] || break
    bn=$(basename "$f" .fail)
    echo "  - $bn"
  done
fi
echo "==========================================="

