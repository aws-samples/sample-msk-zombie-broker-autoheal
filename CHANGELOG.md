# Changelog

All notable changes to **msk-zombie-broker-autoheal** are documented here.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [0.3.0] — 2026-06-30 — Open-sourced to aws-samples + re-validated on live MSK

Published to [`aws-samples/sample-msk-zombie-broker-autoheal`](https://github.com/aws-samples/sample-msk-zombie-broker-autoheal)
after an AWS security review (PCSR) and the Sample Code self-service process, and re-validated
end-to-end on a freshly provisioned live Amazon MSK cluster.

### Added
- GitHub best-practice scaffolding: `SECURITY.md`, issue templates
  (`.github/ISSUE_TEMPLATE/`), `PULL_REQUEST_TEMPLATE.md`, and `dependabot.yml`
  (GitHub Actions, monthly).
- Table of contents in both `README.md` and `README_CN.md`; embedded the self-heal
  sequence diagram in `docs/ARCHITECTURE.md`.

### Changed
- Licensed under **MIT-0** (MIT No Attribution) per aws-samples policy; added the standard
  `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`.
- CI badge and clone paths now point to `aws-samples/sample-msk-zombie-broker-autoheal`.

### Verified (re-validation, 2026-06-30)
- Fresh live E2E on a real MSK cluster (Kafka 3.8.x, ZooKeeper, 3 × kafka.t3.small,
  us-east-1): **E2E PASSED**.
  - Zero false positives while healthy (`action=none`, `urp_positive=false`).
  - Detection fired correctly on a real broker outage: after broker 1 was rebooted,
    `urp_positive` went `true` and the scanner flagged **broker 1** specifically
    (`action=dry_run_would_reboot, broker=1`) — no other broker was mis-flagged.
  - Recovery confirmed (under-replicated partitions returned to 0).
  - Teardown complete with **independently verified zero residual** (MSK, EC2, security
    group, IAM role, and Lambda all removed).
- Offline: 15/15 unit + regression tests, `cfn-lint` clean.
- **Honest harness note:** in this run the Kafka client's `kafka-producer-perf-test.sh`
  was not found at the expected path, so there was no synthetic producer traffic; detection
  was still validated via the real reboot-induced under-replication. This run deployed in
  **observe-only** mode (so the reboot decision was logged as `dry_run_would_reboot`); the
  actual autonomous `kafka:RebootBroker` path was exercised in the 0.2.0 live run.

## [0.2.0] — 2026-06-20 — Validated on a live Amazon MSK cluster

Ran the tool end-to-end against a real provisioned MSK cluster (Kafka 3.8.x,
ZooKeeper, 3 × kafka.t3.small). Full evidence in [`docs/POC-REPORT.md`](docs/POC-REPORT.md).
The live POC surfaced two bugs that would have made the tool silently ineffective.

### Fixed
- **(critical) Detection never fired — `UnderReplicatedPartitions` has no cluster-only
  CloudWatch dimension.** MSK emits this metric only per-broker (`[Cluster Name, Broker ID]`),
  and a *down* broker does not report its own URP (the leaders do). The previous cluster-only
  query always returned empty → `urp_positive` was always `false` → no zombie was ever
  detected. Now `_scan` issues a **per-broker** URP query and `_cluster_under_replicated`
  flags the cluster if **any** broker reports `URP>0`. Added `URP_LOOKBACK_MIN` (default 5)
  so the URP spike and the `BytesIn=0` window don't have to line up minute-for-minute.
- **(deploy) EventBridge `put-targets` rejected `Input={}`** (shorthand parsed it as a dict).
  Removed the `Input` parameter (the Lambda ignores the event payload).

### Added
- **Runtime hardening:** cooldown state is now recorded *before* the `RebootBroker` call and
  the call is wrapped in try/except (`action=reboot_deferred`), so a rejected reboot
  (e.g. "a cluster operation is already in progress") can never cause a tight retry loop.
- **Regression tests** (`tests/test_regression.py`) locking in both fixes + the
  `reboot_deferred` path.
- **ARN parser** extracted to `lib/parse_msk_arn.sh` (single source of truth) with its own
  test (`tests/test_arn_parse.sh`).
- **GitHub Actions CI** (`.github/workflows/ci.yml`): unit + regression tests, `bash -n`,
  shell ARN test, and `cfn-lint` on the SAM template, on every push/PR.
- **Automated live E2E harness** (`tests/e2e_live.sh`): create cluster → deploy → induce a
  real broker outage → assert detection + recovery → teardown. **Actually executed on live
  MSK: E2E PASSED.** The run surfaced (and we fixed) a teardown-robustness bug: after the
  induce step the cluster can be in `REBOOTING_BROKER`, where `delete-cluster` is rejected —
  the teardown now waits for a stable state before deleting and also removes the SSM
  instance-profile/role it created (verified zero residual).
- `docs/POC-REPORT.md` with all real command outputs and an honest scope statement.

### Verified
- Live: deploy + idempotency, zero false positives under real traffic, detection on real
  CloudWatch metrics, an **autonomous real `kafka:RebootBroker`**, real recovery (URP→0),
  the cooldown guardrail, and clean teardown (zero residual).
- Offline: 15/15 unit + regression tests, `cfn-lint` clean, `bash -n` clean.

## [0.1.0] — 2026-06-19 — Initial release

- `selfheal_lambda.py`: poll-based scanner — one Lambda scans all brokers, detects a zombie
  (`BytesInPerSec=0` + cluster under-replication) and issues `kafka:RebootBroker` within
  guardrails (one-broker-only / cooldown / daily-cap / reboot-ineffective → ReplaceNode).
- `deploy.sh`: idempotent one-command deployer (`--plan` / `--observe-only` / `--teardown`).
- `template.yaml`: AWS SAM IaC alternative.
- `tests/test_guardrails.py`: 9 offline guardrail tests.
- `l0-client-hardening/`: producer hardening + topic resilience audit.
- README, `docs/ARCHITECTURE.md`, MIT license.
