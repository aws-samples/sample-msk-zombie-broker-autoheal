# POC Report — Validated on a live Amazon MSK cluster

> **Date:** 2026-06-20 · **Account:** AWS sandbox (us-east-1) · **Cluster:** `msk-poc-zombie`,
> provisioned MSK, **Kafka 3.8.x, ZooKeeper mode**, 3 × `kafka.t3.small`, PER_BROKER +
> Open Monitoring, RF=3 topic with a live `acks=all` producer.
>
> Everything below is **real output from a real managed MSK cluster**, captured during the
> run and then fully torn down (zero residual). This report exists so the tool is
> credible, traceable, and safe to hand to customers.

## TL;DR

| What we set out to prove | Result |
|---|---|
| `deploy.sh` deploys end-to-end on a real cluster, and is **idempotent** | ✅ |
| **Zero false positives** under real traffic | ✅ `action=none, urp_positive=false` |
| Detection **fires on real CloudWatch metrics** when a broker is actually down | ✅ `dry_run_would_reboot, broker:1` |
| The tool **autonomously issues a real `kafka:RebootBroker`** in live mode | ✅ real new ClusterOperationArn |
| `RebootBroker` **actually recovers** the broker (ISR/URP back to healthy) | ✅ URP→0, BytesIn climbs back |
| Guardrails work on real state (**cooldown**) | ✅ `action=cooldown, since_last=24` |
| Clean **teardown**, zero residual | ✅ |
| **Two real bugs found & fixed because we tested for real** | ✅ (see below) |

The single most important outcome: **two bugs that would have made the tool silently
useless were found only by running against real MSK.** That is exactly why this POC was
worth doing.

---

## The validated self-heal loop (sequence)

This is the exact ordering exercised on the live cluster — one EventBridge tick, detect by
symptom, guardrails, `RebootBroker`, ZooKeeper fences the broker, leader re-elected:

![Self-heal loop sequence, as validated on a live MSK cluster](img/sequence-selfheal.png)

## Bugs found by testing for real (both fixed + re-verified)

### Bug 1 (critical) — `UnderReplicatedPartitions` has no cluster-only dimension
The first version queried `UnderReplicatedPartitions` with only the `Cluster Name`
dimension. On a live cluster, `list-metrics` proves MSK emits this metric **only** with
`[Cluster Name, Broker ID]`:

```
$ aws cloudwatch list-metrics --namespace AWS/Kafka --metric-name UnderReplicatedPartitions
[ {"Name":"Cluster Name","Value":"msk-poc-zombie"}, {"Name":"Broker ID","Value":"1"} ]
[ {"Name":"Cluster Name","Value":"msk-poc-zombie"}, {"Name":"Broker ID","Value":"2"} ]
[ {"Name":"Cluster Name","Value":"msk-poc-zombie"}, {"Name":"Broker ID","Value":"3"} ]
# (no cluster-only series exists)
```

So the cluster-only query returned **nothing → `urp_positive` was always false → the tool
would NEVER have detected a zombie.** Also note: a *down* broker does not report its own
URP — the **leader** brokers do. During the broker-1 outage we measured:

```
broker 2 URP (Max/60s): ... 0, 0, 30, 0 ...    <- spikes when broker-1 drops
broker 3 URP (Max/60s): ... 0, 0, 29, 0 ...
broker 1 URP (Max/60s): ... 0, 0, 0,  0 ...     <- the down broker reports nothing
```

**Fix:** query URP **per broker** and treat the cluster as under-replicated if **any**
broker reports `URP>0`, with a slightly longer URP look-back than the BytesIn window
(`URP_LOOKBACK_MIN`, default 5) because the under-replication spike and the `BytesIn=0`
window do not always line up minute-for-minute. (`selfheal_lambda.py: _scan` /
`_cluster_under_replicated`.)

### Bug 2 (deploy) — EventBridge `put-targets Input={}` shorthand error
`--targets "Id=1,Arn=...,Input={}"` makes the CLI parse `{}` as a dict and reject it.
**Fix:** omit `Input` entirely (the Lambda ignores the event payload).

### Hardening added from the run
In live mode, if a cluster operation is already in progress, `RebootBroker` can be
rejected. The tool now records cooldown state **before** the API call and catches the
error (`action=reboot_deferred`) so a rejected reboot can never cause a tight retry loop.

---

## Step-by-step real outputs

### 1. Deploy + idempotency (real cluster)
```
$ ./deploy.sh --cluster-arn <msk-poc-zombie ARN> --observe-only --yes
  ✓ cluster reachable · brokers=3 · monitoring=PER_BROKER
  ✓ DynamoDB state table created   ✓ SNS topic   ✓ IAM role created   ✓ Lambda created   ✓ scheduled
# re-run (idempotency):
  ✓ exists   ✓ role exists   ✓ updated   ✓ scheduled        # no errors, in-place update
```

### 2. Zero false positive (healthy + live traffic)
All three brokers had `BytesInPerSec > 0` (steady producer), cluster URP = 0:
```
$ aws lambda invoke --function-name msk-autoheal-fn ...
{"action": "none", "brokers": 3, "urp_positive": false}
```

### 3. Detection on real metrics (observe-only, window=1) — induced real broker-1 outage
```
$ aws kafka reboot-broker --broker-ids 1        # create a REAL broker-down event
[00:42] b1_BytesIn=11484  Lambda={"action":"none","urp_positive":true}   <- URP detected via per-broker aggregation
[00:43] b1_BytesIn=0.0    Lambda={"action":"dry_run_would_reboot","broker":1}   <- DETECTION FIRED, correct broker
```
Naive checks (process/port/ZK) stayed green the whole time; only the symptom signal caught it.

### 4. Recovery proof
```
broker-1 BytesIn: 0.0 -> 4995 -> 8105        (climbs back)
under-replicated partition count: 0          (kafka-topics.sh --under-replicated-partitions)
```

### 5. LIVE autonomous self-heal (the tool issues a real RebootBroker) — induced on broker-2
```
$ ./deploy.sh ... --window 1 --yes            # LIVE mode (DRY_RUN=false)
[00:52] b2_BytesIn=0.0  Lambda={"action":"reboot_broker","broker":2,
                                 "op":"arn:aws:kafka:...cluster-operation/.../a85b289f-..."}
```
The Lambda **autonomously called `kafka:RebootBroker` and received a real ClusterOperationArn**
(distinct from the operator-induced one).

### 6. Guardrail (cooldown) on real state
```
DynamoDB msk-autoheal-state / broker#2:  consecutive=1  reboots_today=1  last_reboot=1781916737  day=2026-06-20
$ aws lambda invoke ...        # immediate re-invoke
{"action": "cooldown", "broker": 2, "since_last": 24}     <- refuses to re-reboot a recovering broker
```

### 7. Teardown (zero residual)
```
$ ./deploy.sh --cluster-arn <ARN> --teardown --yes
  ✓ rule   ✓ function   ✓ role   ✓ table   ✓ topic   ✓ Teardown complete.
$ aws kafka delete-cluster ...        # after cluster returned to ACTIVE
$ aws ec2 terminate-instances ...     # client EC2
# verified: no MSK clusters, no autoheal resources, EC2 terminated, SG removed.
```

---

## What this POC does and does NOT prove (honest scope)

- **Proven on real managed MSK:** deploy/idempotency, zero false positive, detection on
  real CloudWatch metrics, autonomous real `RebootBroker`, real recovery, cooldown guardrail,
  clean teardown — plus the two bug fixes above.
- **Not reproducible on managed MSK:** the *originating* app-log volume stall itself, because
  the EBS volume/host is AWS-internal and uninjectable. The real stall → zombie mechanism was
  reproduced separately on **self-managed Kafka** (dm-delay block-layer injection: `dd` stuck
  in uninterruptible D-state, ZK session alive / no fence, JMX endpoint collapse, `produce`
  FAIL, recovery on restart). See the project's `docs/ARCHITECTURE.md` and the upstream RCA.
- The induced outages here use `RebootBroker` to create a real `BytesIn=0 + URP>0` condition
  (the closest reproducible proxy for a zombie on managed MSK); the multi-broker "LSE"
  no-auto-action guardrail is covered by the unit tests (we intentionally do not take two
  real brokers down at once).

Combining the two tracks: the **mechanism** is proven on self-managed Kafka, and the
**shipped tool's deploy + detection + autonomous recovery + guardrails** are proven on real
managed MSK. That is the credible, end-to-end evidence base for releasing this to customers.
