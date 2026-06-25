#!/usr/bin/env bash
# =============================================================================
# msk-zombie-broker-autoheal — one-command deployer
# -----------------------------------------------------------------------------
# Deploys a poll-based self-heal stack for the MSK "app-log volume blind spot":
# every minute an EventBridge schedule triggers ONE Lambda that scans all brokers,
# detects a "zombie" (BytesInPerSec=0 + cluster UnderReplicatedPartitions>0), and
# — within strict guardrails — issues kafka:RebootBroker to force a ZK fence +
# leader election. Pure client-side, no migration, broker-count-agnostic.
#
# Creates (all prefixed, easy teardown):
#   - IAM role (least privilege)        ${PREFIX}-role
#   - Lambda (selfheal_lambda.py)       ${PREFIX}-fn
#   - DynamoDB state table (PPR + TTL)  ${PREFIX}-state
#   - SNS alerts topic (+ optional sub) ${PREFIX}-alerts
#   - EventBridge rate(1 min) schedule  ${PREFIX}-schedule
# Also enables MSK PER_BROKER + Open Monitoring (idempotent) for the metrics.
#
# Usage:
#   ./deploy.sh --cluster-arn <ARN> [options]
#     --region <r>            (default: parsed from ARN)
#     --notify-email <addr>   subscribe an email to the alerts topic
#     --cooldown <sec>        per-broker reboot cooldown   (default 600)
#     --daily-cap <n>         max reboots/cluster/day      (default 4)
#     --window <min>          detection window in minutes  (default 3)
#     --prefix <name>         resource name prefix         (default msk-autoheal)
#     --observe-only          deploy in OBSERVE mode (detect+notify, NEVER reboot)
#     --plan                  print intended AWS actions, make NO changes
#     --teardown              delete everything this tool created
#     --yes                   skip confirmation prompts
#
# Safe rollout: deploy --observe-only first, watch the alerts for a few days,
# then re-run without --observe-only to enable automatic reboot.
# =============================================================================
set -euo pipefail

CLUSTER_ARN="" ; REGION="" ; NOTIFY_EMAIL="" ; PREFIX="msk-autoheal"
COOLDOWN=600 ; DAILY_CAP=4 ; WINDOW=3
OBSERVE_ONLY=false ; PLAN=false ; TEARDOWN=false ; ASSUME_YES=false

while [[ $# -gt 0 ]]; do case "$1" in
  --cluster-arn)  CLUSTER_ARN="$2"; shift 2;;
  --region)       REGION="$2"; shift 2;;
  --notify-email) NOTIFY_EMAIL="$2"; shift 2;;
  --cooldown)     COOLDOWN="$2"; shift 2;;
  --daily-cap)    DAILY_CAP="$2"; shift 2;;
  --window)       WINDOW="$2"; shift 2;;
  --prefix)       PREFIX="$2"; shift 2;;
  --observe-only) OBSERVE_ONLY=true; shift;;
  --plan)         PLAN=true; shift;;
  --teardown)     TEARDOWN=true; shift;;
  --yes)          ASSUME_YES=true; shift;;
  -h|--help)      sed -n '2,40p' "$0"; exit 0;;
  *) echo "Unknown option: $1" >&2; exit 2;;
esac; done

[[ -z "$CLUSTER_ARN" ]] && { echo "ERROR: --cluster-arn is required" >&2; exit 2; }

# Parse region / cluster name / account from the ARN:
# arn:aws:kafka:REGION:ACCOUNT:cluster/NAME/UUID
# Parse region / cluster name / account from the ARN (single source of truth: lib/parse_msk_arn.sh)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/parse_msk_arn.sh"
if ! PARSED="$(parse_msk_arn "$CLUSTER_ARN")"; then
  echo "ERROR: not a valid MSK cluster ARN: $CLUSTER_ARN" >&2; exit 2
fi
read -r _svc ARN_REGION ACCOUNT CLUSTER_NAME <<< "$PARSED"
[[ -z "$REGION" ]] && REGION="$ARN_REGION"

ROLE="${PREFIX}-role"; FN="${PREFIX}-fn"; TBL="${PREFIX}-state"
TOPIC="${PREFIX}-alerts"; RULE="${PREFIX}-schedule"
AWSCLI=(aws --region "$REGION")
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

say()  { printf '\033[1;36m▸ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }
confirm() { $ASSUME_YES && return 0; read -rp "$1 [y/N] " a; [[ "$a" == y || "$a" == Y ]]; }

echo "──────────────────────────────────────────────────────────────"
echo " MSK zombie-broker auto-heal · ${PREFIX}"
echo "   cluster : $CLUSTER_NAME"
echo "   region  : $REGION   account: $ACCOUNT"
echo "   mode    : $([[ $TEARDOWN == true ]] && echo TEARDOWN || ([[ $PLAN == true ]] && echo PLAN || ([[ $OBSERVE_ONLY == true ]] && echo 'DEPLOY (observe-only)' || echo 'DEPLOY (live self-heal)')))"
echo "──────────────────────────────────────────────────────────────"

# ----------------------------------------------------------------- TEARDOWN
if $TEARDOWN; then
  confirm "Delete all '${PREFIX}-*' resources for $CLUSTER_NAME?" || { echo aborted; exit 0; }
  say "Removing EventBridge schedule"
  "${AWSCLI[@]}" events remove-targets --rule "$RULE" --ids 1 >/dev/null 2>&1 || true
  "${AWSCLI[@]}" events delete-rule --name "$RULE" >/dev/null 2>&1 || true; ok "rule"
  say "Removing Lambda"
  "${AWSCLI[@]}" lambda delete-function --function-name "$FN" >/dev/null 2>&1 || true; ok "function"
  say "Removing IAM role"
  "${AWSCLI[@]}" iam delete-role-policy --role-name "$ROLE" --policy-name "${PREFIX}-policy" >/dev/null 2>&1 || true
  "${AWSCLI[@]}" iam delete-role --role-name "$ROLE" >/dev/null 2>&1 || true; ok "role"
  say "Removing DynamoDB table"; "${AWSCLI[@]}" dynamodb delete-table --table-name "$TBL" >/dev/null 2>&1 || true; ok "table"
  say "Removing SNS topic"
  TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:${TOPIC}"
  "${AWSCLI[@]}" sns delete-topic --topic-arn "$TOPIC_ARN" >/dev/null 2>&1 || true; ok "topic"
  echo; ok "Teardown complete. (MSK monitoring level left unchanged — adjust manually if desired.)"
  exit 0
fi

# ----------------------------------------------------------------- PRECHECK
say "Preflight"
"${AWSCLI[@]}" sts get-caller-identity >/dev/null || { echo "ERROR: AWS credentials not working" >&2; exit 1; }
CINFO="$("${AWSCLI[@]}" kafka describe-cluster --cluster-arn "$CLUSTER_ARN" 2>/dev/null)" \
  || { echo "ERROR: cannot describe cluster (check ARN/region/permissions)" >&2; exit 1; }
NBROKERS="$(printf '%s' "$CINFO" | grep -o '"NumberOfBrokerNodes"[: ]*[0-9]*' | grep -o '[0-9]*' | head -1)"
CURVER="$(printf '%s' "$CINFO" | sed -nE 's/.*"CurrentVersion"[: ]*"([^"]+)".*/\1/p' | head -1)"
ENHMON="$(printf '%s' "$CINFO" | sed -nE 's/.*"EnhancedMonitoring"[: ]*"([^"]+)".*/\1/p' | head -1)"
ok "cluster reachable · brokers=${NBROKERS:-?} · monitoring=${ENHMON:-?} · version=${CURVER:-?}"

DRY_RUN_ENV=$([[ $OBSERVE_ONLY == true ]] && echo true || echo false)

if $PLAN; then
  cat <<EOF

PLAN — would create / ensure (no changes made):
  • MSK update-monitoring → PER_BROKER + Open Monitoring (if not already)
  • DynamoDB table         $TBL  (PAY_PER_REQUEST, TTL on 'ttl')
  • SNS topic              $TOPIC $( [[ -n $NOTIFY_EMAIL ]] && echo "(+ email sub: $NOTIFY_EMAIL)")
  • IAM role               $ROLE  (kafka:DescribeCluster/RebootBroker on this cluster,
                                    cloudwatch:GetMetricData, ddb on table, sns:Publish, logs)
  • Lambda                 $FN    (python3.12, 60s, env DRY_RUN=$DRY_RUN_ENV, COOLDOWN=$COOLDOWN,
                                    DAILY_CAP=$DAILY_CAP, WINDOW=$WINDOW)
  • EventBridge rule       $RULE  (rate(1 minute) → $FN)
EOF
  exit 0
fi

confirm "Proceed to deploy into account $ACCOUNT / $REGION?" || { echo aborted; exit 0; }

# ----------------------------------------------------------------- 1. monitoring
say "Ensuring PER_BROKER + Open Monitoring"
case "$ENHMON" in
  PER_BROKER|PER_TOPIC_PER_BROKER|PER_TOPIC_PER_PARTITION) ok "enhanced monitoring already $ENHMON";;
  *)
    if [[ -n "$CURVER" ]]; then
      "${AWSCLI[@]}" kafka update-monitoring --cluster-arn "$CLUSTER_ARN" --current-version "$CURVER" \
        --enhanced-monitoring PER_BROKER \
        --open-monitoring '{"Prometheus":{"JmxExporter":{"EnabledInBroker":true},"NodeExporter":{"EnabledInBroker":true}}}' \
        >/dev/null && warn "monitoring update started (rolling cluster op; metrics appear shortly)"
    else warn "could not read CurrentVersion; enable PER_BROKER monitoring manually"; fi;;
esac

# ----------------------------------------------------------------- 2. DynamoDB
say "DynamoDB state table"
if "${AWSCLI[@]}" dynamodb describe-table --table-name "$TBL" >/dev/null 2>&1; then ok "exists"; else
  "${AWSCLI[@]}" dynamodb create-table --table-name "$TBL" \
    --attribute-definitions AttributeName=clusterArn,AttributeType=S AttributeName=brokerSk,AttributeType=S \
    --key-schema AttributeName=clusterArn,KeyType=HASH AttributeName=brokerSk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST >/dev/null
  "${AWSCLI[@]}" dynamodb wait table-exists --table-name "$TBL"
  "${AWSCLI[@]}" dynamodb update-time-to-live --table-name "$TBL" \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null 2>&1 || true
  ok "created"
fi

# ----------------------------------------------------------------- 3. SNS
say "SNS alerts topic"
TOPIC_ARN="$("${AWSCLI[@]}" sns create-topic --name "$TOPIC" --query TopicArn --output text)"
ok "$TOPIC_ARN"
if [[ -n "$NOTIFY_EMAIL" ]]; then
  "${AWSCLI[@]}" sns subscribe --topic-arn "$TOPIC_ARN" --protocol email --notification-endpoint "$NOTIFY_EMAIL" >/dev/null
  warn "confirm the subscription email sent to $NOTIFY_EMAIL"
fi

# ----------------------------------------------------------------- 4. IAM role
say "IAM role + least-privilege policy"
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
if ! "${AWSCLI[@]}" iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  "${AWSCLI[@]}" iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" >/dev/null
  ok "role created"
else ok "role exists"; fi
POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"DescribeAndReboot","Effect":"Allow","Action":["kafka:DescribeCluster","kafka:RebootBroker"],"Resource":"$CLUSTER_ARN"},
 {"Sid":"ReadMetrics","Effect":"Allow","Action":["cloudwatch:GetMetricData"],"Resource":"*"},
 {"Sid":"State","Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:PutItem"],"Resource":"arn:aws:dynamodb:${REGION}:${ACCOUNT}:table/${TBL}"},
 {"Sid":"Notify","Effect":"Allow","Action":["sns:Publish"],"Resource":"$TOPIC_ARN"},
 {"Sid":"Logs","Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"arn:aws:logs:${REGION}:${ACCOUNT}:*"}
]}
JSON
)
"${AWSCLI[@]}" iam put-role-policy --role-name "$ROLE" --policy-name "${PREFIX}-policy" --policy-document "$POLICY" >/dev/null
ok "policy attached"
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${ROLE}"

# ----------------------------------------------------------------- 5. Lambda
say "Packaging + deploying Lambda"
TMP="$(mktemp -d)"; cp "$HERE/selfheal_lambda.py" "$TMP/"; ( cd "$TMP" && zip -q fn.zip selfheal_lambda.py )
ENV_VARS="Variables={CLUSTER_ARN=$CLUSTER_ARN,CLUSTER_NAME=$CLUSTER_NAME,REGION=$REGION,STATE_TABLE=$TBL,SNS_TOPIC_ARN=$TOPIC_ARN,COOLDOWN_S=$COOLDOWN,DAILY_CAP=$DAILY_CAP,DETECT_WINDOW_MIN=$WINDOW,DRY_RUN=$DRY_RUN_ENV}"
if "${AWSCLI[@]}" lambda get-function --function-name "$FN" >/dev/null 2>&1; then
  "${AWSCLI[@]}" lambda update-function-code --function-name "$FN" --zip-file "fileb://$TMP/fn.zip" >/dev/null
  "${AWSCLI[@]}" lambda wait function-updated --function-name "$FN"
  "${AWSCLI[@]}" lambda update-function-configuration --function-name "$FN" \
    --handler selfheal_lambda.handler --runtime python3.12 --timeout 60 --memory-size 256 \
    --role "$ROLE_ARN" --environment "$ENV_VARS" >/dev/null
  ok "updated"
else
  # role propagation can lag; retry create briefly
  for i in 1 2 3 4 5; do
    if "${AWSCLI[@]}" lambda create-function --function-name "$FN" --runtime python3.12 \
        --role "$ROLE_ARN" --handler selfheal_lambda.handler --timeout 60 --memory-size 256 \
        --zip-file "fileb://$TMP/fn.zip" --environment "$ENV_VARS" >/dev/null 2>&1; then ok "created"; break; fi
      warn "waiting for IAM role propagation ($i/5)"; sleep 6
      [[ $i == 5 ]] && { echo "ERROR: lambda create failed" >&2; exit 1; }
  done
fi
FN_ARN="$("${AWSCLI[@]}" lambda get-function --function-name "$FN" --query Configuration.FunctionArn --output text)"
rm -rf "$TMP"

# ----------------------------------------------------------------- 6. EventBridge
say "EventBridge rate(1 minute) schedule"
"${AWSCLI[@]}" events put-rule --name "$RULE" --schedule-expression "rate(1 minute)" \
  --description "Trigger $FN to scan MSK $CLUSTER_NAME for zombie brokers" >/dev/null
"${AWSCLI[@]}" lambda add-permission --function-name "$FN" --statement-id "${PREFIX}-eb" \
  --action lambda:InvokeFunction --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT}:rule/${RULE}" >/dev/null 2>&1 || true
"${AWSCLI[@]}" events put-targets --rule "$RULE" --targets "Id=1,Arn=$FN_ARN" >/dev/null
ok "scheduled"

# ----------------------------------------------------------------- summary
echo "──────────────────────────────────────────────────────────────"
ok "Deployed. Mode: $([[ $OBSERVE_ONLY == true ]] && echo 'OBSERVE-ONLY (detect + notify, NO reboot)' || echo 'LIVE self-heal')"
cat <<EOF

Watch it work:
  aws --region $REGION logs tail /aws/lambda/$FN --follow
Manually invoke once (test the scan):
  aws --region $REGION lambda invoke --function-name $FN /dev/stdout
$([[ $OBSERVE_ONLY == true ]] && echo "When confident, enable live healing:
  ./deploy.sh --cluster-arn $CLUSTER_ARN   # (re-run without --observe-only)")
Tear it all down:
  ./deploy.sh --cluster-arn $CLUSTER_ARN --teardown --prefix $PREFIX
EOF
echo "──────────────────────────────────────────────────────────────"
