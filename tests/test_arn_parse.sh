#!/usr/bin/env bash
# Unit test for lib/parse_msk_arn.sh (the ARN parser deploy.sh relies on).
# Run: bash tests/test_arn_parse.sh
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../lib/parse_msk_arn.sh"

pass=0; fail=0
check() { # check <desc> <expected> <actual>
  if [[ "$2" == "$3" ]]; then echo "  ok: $1"; pass=$((pass+1))
  else echo "  FAIL: $1 — expected [$2] got [$3]"; fail=$((fail+1)); fi
}

# valid ARN
OUT="$(parse_msk_arn 'arn:aws:kafka:us-east-1:111122223333:cluster/demo-cluster/abcd1234-5678-90ab-cdef-1234567890ab-25')"
read -r svc region acct name <<< "$OUT"
check "service"  "kafka"          "$svc"
check "region"   "us-east-1"      "$region"
check "account"  "111122223333"   "$acct"
check "name"     "demo-cluster"   "$name"

# another region/name
OUT="$(parse_msk_arn 'arn:aws:kafka:ap-northeast-1:123456789012:cluster/data-prod-msk/abc-1')"
read -r svc region acct name <<< "$OUT"
check "region2"  "ap-northeast-1" "$region"
check "name2"    "data-prod-msk"  "$name"

# non-kafka ARN must fail (return non-zero)
if parse_msk_arn 'arn:aws:sns:us-east-1:111:topic' >/dev/null 2>&1; then
  echo "  FAIL: non-kafka ARN should be rejected"; fail=$((fail+1))
else echo "  ok: rejects non-kafka ARN"; pass=$((pass+1)); fi

# garbage must fail
if parse_msk_arn 'not-an-arn' >/dev/null 2>&1; then
  echo "  FAIL: garbage should be rejected"; fail=$((fail+1))
else echo "  ok: rejects garbage"; pass=$((pass+1)); fi

echo "----"; echo "passed=$pass failed=$fail"
[[ $fail -eq 0 ]] || exit 1
