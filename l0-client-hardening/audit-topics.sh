#!/usr/bin/env bash
# =============================================================================
# audit-topics.sh — flag topics that are NOT resilient to a single broker loss.
# A topic survives one zombie/stalled broker only if RF>=3 AND min.insync.replicas>=2
# (paired with producer acks=all). This read-only audit lists violators.
#
# Requires: kafka CLI tools on PATH (kafka-topics.sh, kafka-configs.sh) and
# connectivity to the bootstrap brokers (with your client.properties for auth).
#
# Usage:
#   ./audit-topics.sh -b <bootstrap:9092> [-c client.properties]
# =============================================================================
set -euo pipefail
BOOT="" ; CLIENT=""
while [[ $# -gt 0 ]]; do case "$1" in
  -b) BOOT="$2"; shift 2;; -c) CLIENT="$2"; shift 2;;
  -h|--help) sed -n '2,14p' "$0"; exit 0;; *) echo "unknown: $1"; exit 2;;
esac; done
[[ -z "$BOOT" ]] && { echo "ERROR: -b <bootstrap> required" >&2; exit 2; }
CMD=(--bootstrap-server "$BOOT"); [[ -n "$CLIENT" ]] && CMD+=(--command-config "$CLIENT")

echo "Auditing topics on $BOOT (RF>=3 and min.insync.replicas>=2 required)..."
printf '%-45s %4s %6s %s\n' "TOPIC" "RF" "MINISR" "VERDICT"
bad=0
while read -r topic; do
  [[ -z "$topic" || "$topic" == __* ]] && continue
  rf=$(kafka-topics.sh "${CMD[@]}" --describe --topic "$topic" 2>/dev/null \
        | sed -nE 's/.*ReplicationFactor: *([0-9]+).*/\1/p' | head -1)
  isr=$(kafka-configs.sh "${CMD[@]}" --entity-type topics --entity-name "$topic" --describe 2>/dev/null \
        | sed -nE 's/.*min.insync.replicas=([0-9]+).*/\1/p' | head -1)
  isr=${isr:-1}; rf=${rf:-0}
  if (( rf >= 3 && isr >= 2 )); then verdict="ok"; else verdict="!! NOT RESILIENT"; bad=$((bad+1)); fi
  printf '%-45s %4s %6s %s\n' "$topic" "$rf" "$isr" "$verdict"
done < <(kafka-topics.sh "${CMD[@]}" --list 2>/dev/null)

echo
if (( bad > 0 )); then
  echo "$bad topic(s) are NOT resilient to a single broker loss."
  echo "Fix examples:"
  echo "  kafka-configs.sh ${CMD[*]} --entity-type topics --entity-name <T> --alter --add-config min.insync.replicas=2"
  echo "  # RF increase needs a reassignment plan (kafka-reassign-partitions.sh)"
  exit 1
fi
echo "All topics resilient. (Pair with producer acks=all — see producer.properties.)"
