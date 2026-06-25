"""
MSK App-Log Volume Blind Spot — Self-Heal Lambda (poll-based, guardrailed)
==========================================================================
Covers AWS Defect A (monitoring blind spot) + bypasses Defect B (ZK never fences
a disk-stalled "zombie" broker) by detecting the zombie via symptom metrics and
issuing kafka:RebootBroker — which forces the ZK session to drop, triggering a
clean leader election.

WHY POLL-BASED (one Lambda, broker-count-agnostic):
  Instead of N per-broker CloudWatch composite alarms + EventBridge brokerId
  routing (O(N) moving parts), a single EventBridge schedule (every 1 min) drives
  ONE Lambda that scans every broker. Adding/removing brokers needs zero changes.

DETECTION (the documented, verified symptom signal — see SOLUTION-v2 §3/§6):
  A broker is a zombie when, over the last DETECT_WINDOW_MIN minutes:
     per-broker BytesInPerSec == 0   (PER_BROKER monitoring required)
     AND cluster UnderReplicatedPartitions > 0
  (process/port/ZK all stay "green" during a stall, so naive checks are blind.)

GUARDRAILS (mandatory — see SOLUTION-v2 §4 L2):
  - Act on at most ONE broker per run. >1 zombie at once => suspected LSE =>
    DO NOT auto-act, page a human via SNS.
  - Cooldown per broker (default 600s): never reboot the same broker again before
    it has had a chance to recover.
  - Daily cap per cluster (default 4): beyond it, escalate (L3) instead of looping.
  - If a broker was already rebooted once and is STILL zombie after cooldown, the
    reboot is ineffective (= hardware-level volume) => escalate L3 (request
    ReplaceNode via Sev-2), do NOT reboot again.
  - DRY_RUN mode logs/notifies the intended action without calling RebootBroker.

STATE: a DynamoDB table (PK=clusterArn, SK="broker#<id>") tracks last_reboot_epoch,
reboots_today, day_stamp, consecutive_reboots. Items carry a TTL for auto-cleanup.

ENV VARS (set by deploy.sh):
  CLUSTER_ARN, CLUSTER_NAME, REGION, STATE_TABLE, SNS_TOPIC_ARN,
  COOLDOWN_S=600, DAILY_CAP=4, DETECT_WINDOW_MIN=3, DRY_RUN=false
"""
import os
import time
import json
import datetime
import boto3

REGION        = os.environ.get("REGION") or os.environ.get("AWS_REGION")
CLUSTER_ARN   = os.environ["CLUSTER_ARN"]
CLUSTER_NAME  = os.environ["CLUSTER_NAME"]
STATE_TABLE   = os.environ["STATE_TABLE"]
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
COOLDOWN_S    = int(os.environ.get("COOLDOWN_S", "600"))
DAILY_CAP     = int(os.environ.get("DAILY_CAP", "4"))
WINDOW_MIN    = int(os.environ.get("DETECT_WINDOW_MIN", "3"))
URP_LOOKBACK_MIN = int(os.environ.get("URP_LOOKBACK_MIN", "5"))
DRY_RUN       = os.environ.get("DRY_RUN", "false").lower() == "true"

kafka = boto3.client("kafka", region_name=REGION)
cw    = boto3.client("cloudwatch", region_name=REGION)
ddb   = boto3.client("dynamodb", region_name=REGION)
sns   = boto3.client("sns", region_name=REGION) if SNS_TOPIC_ARN else None


# ----------------------------------------------------------------------------- helpers
def _notify(subject, msg):
    print(f"[NOTIFY] {subject}: {msg}")
    if sns:
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=msg)
        except Exception as e:  # never let notification failure break the heal path
            print(f"[WARN] SNS publish failed: {e}")


def _broker_count():
    """Number of broker nodes from describe-cluster (broker IDs are 1..N)."""
    d = kafka.describe_cluster(ClusterArn=CLUSTER_ARN)["ClusterInfo"]
    return int(d["NumberOfBrokerNodes"]), d.get("State", "UNKNOWN")


def _metric_query(qid, metric, dims, stat):
    return {
        "Id": qid,
        "MetricStat": {
            "Metric": {"Namespace": "AWS/Kafka", "MetricName": metric, "Dimensions": dims},
            "Period": 60,
            "Stat": stat,
        },
        "ReturnData": True,
    }


def _scan(n_brokers):
    """One GetMetricData call: per-broker BytesInPerSec(Sum) + per-broker URP(Maximum).

    IMPORTANT (verified on live MSK): AWS/Kafka UnderReplicatedPartitions is emitted
    ONLY with dimensions [Cluster Name, Broker ID] — there is NO cluster-only series.
    A down/zombie broker does not report its own URP; the *leader* brokers report the
    under-replication. So we query URP per broker and treat the cluster as
    under-replicated if ANY broker reports URP>0. We also look back slightly further
    for URP than for BytesIn, because the under-replication spike and the BytesIn=0
    window do not always line up minute-for-minute during a broker outage.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    fetch_min = max(WINDOW_MIN, URP_LOOKBACK_MIN) + 1
    start = now - datetime.timedelta(minutes=fetch_min)
    queries = []
    for b in range(1, n_brokers + 1):
        dims = [{"Name": "Cluster Name", "Value": CLUSTER_NAME}, {"Name": "Broker ID", "Value": str(b)}]
        queries.append(_metric_query(f"bin{b}", "BytesInPerSec", dims, "Sum"))
        queries.append(_metric_query(f"urp{b}", "UnderReplicatedPartitions", dims, "Maximum"))
    res = {}
    token = None
    while True:
        kw = dict(MetricDataQueries=queries, StartTime=start, EndTime=now, ScanBy="TimestampDescending")
        if token:
            kw["NextToken"] = token
        r = cw.get_metric_data(**kw)
        for m in r["MetricDataResults"]:
            res.setdefault(m["Id"], []).extend(m["Values"])
        token = r.get("NextToken")
        if not token:
            break
    return res


def _cluster_under_replicated(res, n_brokers):
    """True if ANY broker reports URP>0 within the URP lookback window."""
    for b in range(1, n_brokers + 1):
        if any(v > 0 for v in res.get(f"urp{b}", [])[:URP_LOOKBACK_MIN]):
            return True
    return False


def _is_zombie(res, b, urp_positive):
    """Zombie iff we have >=WINDOW_MIN recent datapoints all == 0 AND cluster URP>0.
    Requiring real datapoints (not just 'missing') avoids false-positives on a
    broker that simply has no metrics published yet."""
    vals = res.get(f"bin{b}", [])
    recent = vals[:WINDOW_MIN]
    return bool(urp_positive) and len(recent) >= WINDOW_MIN and all(v == 0 for v in recent)


# ----------------------------------------------------------------------------- state (DDB)
def _key(b):
    return {"clusterArn": {"S": CLUSTER_ARN}, "brokerSk": {"S": f"broker#{b}"}}


def _load(b):
    it = ddb.get_item(TableName=STATE_TABLE, Key=_key(b)).get("Item")
    if not it:
        return {"last_reboot": 0, "reboots_today": 0, "day": "", "consecutive": 0}
    return {
        "last_reboot": int(it.get("last_reboot", {"N": "0"})["N"]),
        "reboots_today": int(it.get("reboots_today", {"N": "0"})["N"]),
        "day": it.get("day", {"S": ""})["S"],
        "consecutive": int(it.get("consecutive", {"N": "0"})["N"]),
    }


def _save(b, st):
    ttl = int(time.time()) + 14 * 24 * 3600  # auto-clean after 14 days
    ddb.put_item(TableName=STATE_TABLE, Item={
        **_key(b),
        "last_reboot": {"N": str(st["last_reboot"])},
        "reboots_today": {"N": str(st["reboots_today"])},
        "day": {"S": st["day"]},
        "consecutive": {"N": str(st["consecutive"])},
        "ttl": {"N": str(ttl)},
    })


def _reset_consecutive(b):
    """Broker is healthy again — clear the 'reboot ineffective' counter."""
    st = _load(b)
    if st["consecutive"] != 0:
        st["consecutive"] = 0
        _save(b, st)


# ----------------------------------------------------------------------------- handler
def handler(event, _ctx):
    now = int(time.time())
    today = datetime.date.today().isoformat()
    n, state = _broker_count()
    print(f"[SCAN] cluster={CLUSTER_NAME} brokers={n} state={state} dry_run={DRY_RUN}")

    res = _scan(n)
    urp_positive = _cluster_under_replicated(res, n)

    zombies = [b for b in range(1, n + 1) if _is_zombie(res, b, urp_positive)]
    healthy = [b for b in range(1, n + 1) if b not in zombies]
    for b in healthy:
        _reset_consecutive(b)

    if not zombies:
        return {"action": "none", "brokers": n, "urp_positive": urp_positive}

    # Guardrail 1: more than one zombie at once => suspected region-level event (LSE).
    if len(zombies) > 1:
        _notify(
            f"[MSK CRITICAL] {CLUSTER_NAME}: {len(zombies)} zombie brokers — NOT auto-acting",
            f"Brokers {zombies} all show BytesIn=0 + cluster URP>0. Multiple brokers "
            f"down simultaneously suggests an AZ/region-level event (LSE). "
            f"Manual intervention required. Cluster: {CLUSTER_ARN}",
        )
        return {"action": "escalate_lse", "zombies": zombies}

    b = zombies[0]
    st = _load(b)
    if st["day"] != today:  # daily counter reset
        st["day"], st["reboots_today"] = today, 0

    # Guardrail 2: cooldown — give a just-rebooted broker time to recover.
    if now - st["last_reboot"] < COOLDOWN_S:
        return {"action": "cooldown", "broker": b, "since_last": now - st["last_reboot"]}

    # Guardrail 3: reboot already attempted and broker is STILL zombie after cooldown
    # => reboot is ineffective (hardware-level volume) => escalate L3, do not loop.
    if st["consecutive"] >= 1:
        _notify(
            f"[MSK L3 ESCALATE] {CLUSTER_NAME}: broker {b} still zombie after reboot",
            f"RebootBroker did NOT recover broker {b} (BytesIn still 0, URP>0). This is a "
            f"hardware-level app-log volume failure. Open an AWS Support case (Sev-2) for the "
            f"MSK service team and request a node replacement (ReplaceNode). Cluster: {CLUSTER_ARN}",
        )
        return {"action": "escalate_replacenode", "broker": b}

    # Guardrail 4: daily cap.
    if st["reboots_today"] >= DAILY_CAP:
        _notify(
            f"[MSK L3 ESCALATE] {CLUSTER_NAME}: daily reboot cap reached",
            f"Broker {b} zombie but daily cap ({DAILY_CAP}) hit. Escalate to humans / "
            f"request ReplaceNode. Cluster: {CLUSTER_ARN}",
        )
        return {"action": "daily_cap", "broker": b}

    # Passed all guardrails — heal.
    if DRY_RUN:
        _notify(
            f"[MSK DRY-RUN] {CLUSTER_NAME}: would RebootBroker {b}",
            f"Confirmed zombie (BytesIn=0 {WINDOW_MIN}m + URP>0). DRY_RUN=true, no action taken. "
            f"Cluster: {CLUSTER_ARN}",
        )
        return {"action": "dry_run_would_reboot", "broker": b}

    # Passed all guardrails — heal. Always record cooldown state (even on API error)
    # so a rejected/failed reboot can't put us into a tight retry loop.
    st["last_reboot"], st["reboots_today"], st["consecutive"] = now, st["reboots_today"] + 1, st["consecutive"] + 1
    _save(b, st)
    try:
        op = kafka.reboot_broker(ClusterArn=CLUSTER_ARN, BrokerIds=[str(b)])
        opid = op.get("ClusterOperationArn")
        _notify(
            f"[MSK SELF-HEAL] {CLUSTER_NAME}: rebooted zombie broker {b}",
            f"Confirmed zombie (BytesIn=0 {WINDOW_MIN}m + URP>0). Issued kafka:RebootBroker. "
            f"Op={opid}. reboots_today={st['reboots_today']}/{DAILY_CAP}. Cluster: {CLUSTER_ARN}",
        )
        return {"action": "reboot_broker", "broker": b, "op": opid}
    except Exception as e:
        # e.g. a cluster operation is already in progress (the cluster is already
        # cycling that broker). Don't crash; cooldown is already recorded.
        _notify(
            f"[MSK SELF-HEAL] {CLUSTER_NAME}: RebootBroker {b} not issued ({type(e).__name__})",
            f"Confirmed zombie but RebootBroker call returned: {e}. Likely a cluster operation "
            f"is already in progress (broker already cycling). Cooldown recorded; will re-evaluate "
            f"after cooldown. Cluster: {CLUSTER_ARN}",
        )
        return {"action": "reboot_deferred", "broker": b, "error": str(e)[:200]}
