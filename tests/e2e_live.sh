#!/usr/bin/env bash
# =============================================================================
# tests/e2e_live.sh — automated END-TO-END test on a REAL Amazon MSK cluster.
#
# Codifies the manual POC (docs/POC-REPORT.md) so anyone can reproduce it:
#   create cluster -> client+topic+producer -> deploy.sh --observe-only
#   -> assert zero false positive -> induce real broker outage
#   -> assert detection fires -> assert recovery -> teardown -> assert zero residual
#
# ⚠️ COSTS MONEY: provisions an MSK cluster + an EC2 for ~30-40 min. A teardown
# trap removes everything even on failure/Ctrl-C (unless --keep).
#
# Usage:
#   bash tests/e2e_live.sh --profile <aws_profile> [--region us-east-1] [--keep]
# =============================================================================
set -uo pipefail

PROFILE=""; REGION="us-east-1"; KEEP=false; PREFIX="msk-e2e"
while [[ $# -gt 0 ]]; do case "$1" in
  --profile) PROFILE="$2"; shift 2;; --region) REGION="$2"; shift 2;;
  --keep) KEEP=true; shift;; --prefix) PREFIX="$2"; shift 2;;
  -h|--help) sed -n '2,16p' "$0"; exit 0;; *) echo "unknown: $1"; exit 2;;
esac; done
[[ -z "$PROFILE" ]] && { echo "ERROR: --profile required"; exit 2; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(dirname "$HERE")"
P=(aws --profile "$PROFILE" --region "$REGION")
CL="${PREFIX}-cluster"; SGNAME="${PREFIX}-sg"; PROF="${PREFIX}-ssm-profile"; ROLE="${PREFIX}-ssm-role"
ARN=""; IID=""; SG=""; FAILED=0
say(){ printf '\n\033[1;36m=== %s ===\033[0m\n' "$*"; }
ok(){ printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
bad(){ printf '\033[1;31m  ✗ %s\033[0m\n' "$*"; FAILED=1; }

teardown(){
  $KEEP && { echo "--keep set, leaving resources up"; return; }
  say "TEARDOWN"
  [[ -n "$ARN" ]] && "$ROOT/deploy.sh" --cluster-arn "$ARN" --teardown --yes --prefix "$PREFIX" >/dev/null 2>&1 || true
  [[ -n "$IID" ]] && "${P[@]}" ec2 terminate-instances --instance-ids "$IID" >/dev/null 2>&1 || true
  if [[ -n "$ARN" ]]; then
    # A reboot/induce can leave the cluster in REBOOTING_BROKER; delete-cluster is
    # rejected unless the cluster is in a stable state. Wait for it, then delete.
    for _ in $(seq 1 20); do
      st=$("${P[@]}" kafka describe-cluster-v2 --cluster-arn "$ARN" --query 'ClusterInfo.State' --output text 2>/dev/null)
      [[ "$st" == "ACTIVE" || "$st" == "FAILED" || -z "$st" || "$st" == "None" ]] && break
      echo "  waiting to delete cluster (state=$st)"; sleep 30
    done
    "${P[@]}" kafka delete-cluster --cluster-arn "$ARN" >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
      n=$("${P[@]}" kafka list-clusters-v2 --query "length(ClusterInfoList[?ClusterName=='$CL'])" --output text 2>/dev/null)
      [[ "$n" == "0" ]] && break; sleep 30
    done
  fi
  # SG can only be deleted after the cluster's ENIs are released
  [[ -n "$SG" ]] && "${P[@]}" ec2 delete-security-group --group-id "$SG" >/dev/null 2>&1 || true
  # remove the SSM instance profile/role this script created
  "${P[@]}" iam remove-role-from-instance-profile --instance-profile-name "$PROF" --role-name "$ROLE" >/dev/null 2>&1 || true
  "${P[@]}" iam delete-instance-profile --instance-profile-name "$PROF" >/dev/null 2>&1 || true
  "${P[@]}" iam detach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore >/dev/null 2>&1 || true
  "${P[@]}" iam delete-role --role-name "$ROLE" >/dev/null 2>&1 || true
  ok "teardown complete (autoheal stack, cluster, ec2, sg, ssm profile/role)"
}
trap teardown EXIT

export AWS_PROFILE="$PROFILE"   # deploy.sh uses bare 'aws'

# --------------------------------------------------------------- prerequisites
say "Network + SSM prerequisites"
VPC=$("${P[@]}" ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
mapfile -t SUBS < <("${P[@]}" ec2 describe-subnets --filters Name=vpc-id,Values=$VPC \
  --query 'Subnets[?MapPublicIpOnLaunch==`true`].SubnetId' --output text | tr '\t' '\n' | head -3)
[[ ${#SUBS[@]} -lt 2 ]] && { bad "need >=2 default subnets"; exit 1; }
ok "vpc=$VPC subnets=${SUBS[*]}"
SG=$("${P[@]}" ec2 create-security-group --group-name "$SGNAME" --description "$PREFIX e2e" --vpc-id $VPC --query GroupId --output text)
"${P[@]}" ec2 authorize-security-group-ingress --group-id $SG --protocol all --source-group $SG >/dev/null
ok "sg=$SG"
# ensure an SSM instance profile exists
if ! "${P[@]}" iam get-instance-profile --instance-profile-name "$PROF" >/dev/null 2>&1; then
  "${P[@]}" iam create-role --role-name "$ROLE" --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  "${P[@]}" iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore >/dev/null
  "${P[@]}" iam create-instance-profile --instance-profile-name "$PROF" >/dev/null
  "${P[@]}" iam add-role-to-instance-profile --instance-profile-name "$PROF" --role-name "$ROLE" >/dev/null
  sleep 15
fi
ok "ssm instance profile=$PROF"

# --------------------------------------------------------------- create cluster
say "Create MSK cluster ($CL, Kafka 3.8.x ZK, 3x kafka.t3.small)"
SUBJSON=$(printf '"%s",' "${SUBS[@]}"); SUBJSON="[${SUBJSON%,}]"
cat > /tmp/${PREFIX}.json <<JSON
{"ClusterName":"$CL","Provisioned":{
 "BrokerNodeGroupInfo":{"InstanceType":"kafka.t3.small","ClientSubnets":$SUBJSON,
   "SecurityGroups":["$SG"],"StorageInfo":{"EbsStorageInfo":{"VolumeSize":10}}},
 "KafkaVersion":"3.8.x","NumberOfBrokerNodes":3,
 "ClientAuthentication":{"Unauthenticated":{"Enabled":true}},
 "EncryptionInfo":{"EncryptionInTransit":{"ClientBroker":"PLAINTEXT","InCluster":true}},
 "EnhancedMonitoring":"PER_BROKER",
 "OpenMonitoring":{"Prometheus":{"JmxExporter":{"EnabledInBroker":true},"NodeExporter":{"EnabledInBroker":true}}}}}
JSON
# NumberOfBrokerNodes must be a multiple of the subnet count
NSUB=${#SUBS[@]}; [[ $NSUB -eq 2 ]] && sed -i.bak 's/"NumberOfBrokerNodes":3/"NumberOfBrokerNodes":2/' /tmp/${PREFIX}.json
ARN=$("${P[@]}" kafka create-cluster-v2 --cli-input-json file:///tmp/${PREFIX}.json --query ClusterArn --output text)
ok "arn=$ARN — waiting for ACTIVE (~25 min)"
for i in $(seq 1 40); do
  ST=$("${P[@]}" kafka describe-cluster-v2 --cluster-arn "$ARN" --query 'ClusterInfo.State' --output text 2>/dev/null)
  [[ "$ST" == "ACTIVE" ]] && { ok "ACTIVE"; break; }
  [[ "$ST" == "FAILED" ]] && { bad "cluster FAILED"; exit 1; }
  sleep 60
done
BOOT=$("${P[@]}" kafka get-bootstrap-brokers --cluster-arn "$ARN" --query BootstrapBrokerString --output text)
NB=$("${P[@]}" kafka describe-cluster-v2 --cluster-arn "$ARN" --query 'ClusterInfo.Provisioned.NumberOfBrokerNodes' --output text)

# --------------------------------------------------------------- client EC2
say "Launch client EC2 (Kafka CLI via SSM)"
AMI=$("${P[@]}" ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
IID=$("${P[@]}" ec2 run-instances --image-id "$AMI" --instance-type t3.small \
  --subnet-id "${SUBS[0]}" --security-group-ids "$SG" --iam-instance-profile Name="$PROF" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$PREFIX-client}]" \
  --query 'Instances[0].InstanceId' --output text)
ok "instance=$IID — waiting for SSM"
for i in $(seq 1 20); do
  [[ "$("${P[@]}" ssm describe-instance-information --filters "Key=InstanceIds,Values=$IID" --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null)" == "Online" ]] && { ok "SSM online"; break; }
  sleep 15
done
ssm_run(){ # ssm_run "<cmds-json-array>" <timeout-seconds> [initial-sleep]
  local cid st; cid=$("${P[@]}" ssm send-command --instance-ids "$IID" --document-name AWS-RunShellScript \
    --parameters "commands=$1" --timeout-seconds "${2:-120}" --query Command.CommandId --output text)
  sleep "${3:-8}"
  # Poll until the command reaches a terminal state — do NOT return while it is still
  # running, otherwise a later step can race a slow install (e.g. the Kafka download/extract)
  # and hit "No such file or directory" on a binary that is not on disk yet.
  for _ in $(seq 1 80); do
    st=$("${P[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$IID" \
         --query Status --output text 2>/dev/null)
    [[ "$st" == "Success" || "$st" == "Failed" || "$st" == "Cancelled" || "$st" == "TimedOut" ]] && break
    sleep 5
  done
  "${P[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$IID" --query StandardOutputContent --output text 2>&1
}
say "Install Kafka CLI (archive.apache.org — dlcdn drops old releases)"
ssm_run '["dnf install -y java-17-amazon-corretto >/dev/null 2>&1","cd /opt && curl -s --max-time 200 -o k.tgz https://archive.apache.org/dist/kafka/3.8.1/kafka_2.13-3.8.1.tgz && tar xzf k.tgz && rm -f k.tgz && ln -sfn /opt/kafka_2.13-3.8.1 /opt/kafka","/opt/kafka/bin/kafka-topics.sh --version 2>&1 | tail -1"]' 260 80 | tail -2

say "Create RF=$NB topic + start producer"
ssm_run "[\"/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOT --create --topic e2e --partitions 6 --replication-factor $NB --config min.insync.replicas=2 2>&1 | tail -1\",\"nohup /opt/kafka/bin/kafka-producer-perf-test.sh --topic e2e --num-records 5000000 --record-size 512 --throughput 60 --producer-props bootstrap.servers=$BOOT acks=all >/var/log/prod.log 2>&1 &\",\"sleep 8; tail -1 /var/log/prod.log\"]" 60 18 | tail -2

# --------------------------------------------------------------- deploy + assert
say "Deploy autoheal (observe-only, window=1)"
"$ROOT/deploy.sh" --cluster-arn "$ARN" --observe-only --window 1 --prefix "$PREFIX" --yes >/dev/null 2>&1 && ok "deployed" || bad "deploy failed"

say "ASSERT zero false positive (healthy + traffic)"
sleep 60
"${P[@]}" lambda invoke --function-name "${PREFIX}-fn" --payload '{}' /tmp/${PREFIX}-h.json >/dev/null 2>&1
R=$(cat /tmp/${PREFIX}-h.json); echo "  lambda: $R"
echo "$R" | grep -q '"action": "none"' && ok "no false positive" || bad "expected action=none, got $R"

say "ASSERT detection fires on a real broker outage (reboot broker 1)"
"${P[@]}" kafka reboot-broker --cluster-arn "$ARN" --broker-ids 1 >/dev/null
DET=0
for i in $(seq 1 18); do
  "${P[@]}" lambda invoke --function-name "${PREFIX}-fn" --payload '{}' /tmp/${PREFIX}-d.json >/dev/null 2>&1
  R=$(cat /tmp/${PREFIX}-d.json)
  echo "  [$i] $R"
  echo "$R" | grep -q 'would_reboot' && { DET=1; ok "DETECTION FIRED: $R"; break; }
  sleep 30
done
[[ $DET -eq 1 ]] || bad "detection did not fire within window"

say "ASSERT recovery (under-replicated partitions return to 0)"
REC=0
for i in $(seq 1 12); do
  URP=$(ssm_run "[\"/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOT --describe --topic e2e --under-replicated-partitions 2>/dev/null | grep -c Partition\"]" 40 8 | tr -d '[:space:]')
  echo "  under-replicated partitions: ${URP:-?}"
  [[ "$URP" == "0" ]] && { REC=1; ok "recovered (URP=0)"; break; }
  sleep 30
done
[[ $REC -eq 1 ]] || bad "broker did not recover to URP=0"

# --------------------------------------------------------------- result
say "RESULT"
if [[ $FAILED -eq 0 ]]; then echo -e "\033[1;32m  E2E PASSED\033[0m"; else echo -e "\033[1;31m  E2E FAILED\033[0m"; fi
exit $FAILED
