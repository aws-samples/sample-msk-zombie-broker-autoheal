# Roadmap

> Status: the project is **functionally complete** today via the validated AWS CLI deployer
> (`deploy.sh`). The items below are **future enhancements** — recorded now, intentionally
> **not yet implemented**. Order = priority / value-for-effort.

## IaC deployment options (future)

We deliberately ship one validated path today and will add declarative IaC later. Planned, in order:

- [ ] **1. Validate the existing SAM / CloudFormation template on a live cluster.**
  `template.yaml` already exists and is `cfn-lint`-clean, but it has only been *linted*, not
  *deployed*. Highest value-for-effort: run it end-to-end on a real MSK cluster the same way
  `deploy.sh` was validated (deploy → induce a real broker outage → confirm detection +
  RebootBroker recovery → teardown), then add a "validated on live MSK" note for the SAM path.
  This turns "CloudFormation option exists" into "CloudFormation option proven".

- [ ] **2. Add a Terraform module.** Most enterprise/production teams standardize on Terraform
  and won't run an ad-hoc bash script in prod. Native Terraform lowers the adoption barrier the
  most among *new* IaC options. Add `terraform/` with the same resources (Lambda, IAM least-priv,
  DynamoDB state table, SNS topic, EventBridge `rate(1 min)` rule) and the same guardrail env vars.

- [ ] **3. Add an AWS CDK app (optional, lowest marginal value).** CDK synthesizes to
  CloudFormation, so it overlaps heavily with the SAM template already provided. Add only if
  there is real demand from CDK-first teams.

## Cross-cutting notes for when we do the above

- **Parity / drift:** every deploy method (CLI, SAM, Terraform, CDK) must stay in sync — same
  resources, same guardrail environment variables (`COOLDOWN_S`, `DAILY_CAP`, `DETECT_WINDOW_MIN`,
  `URP_LOOKBACK_MIN`, `DRY_RUN`). When we add a second/third method, add a small parity check so
  they can't silently diverge.

- **The `update-monitoring` caveat (important):** enabling **PER_BROKER monitoring + Open
  Monitoring** is a one-time, *imperative* change to the customer's **already-existing** MSK
  cluster — a resource that is **not** managed by our stack. Declarative IaC (CFN/Terraform/CDK)
  does not naturally "modify an out-of-band existing cluster's monitoring level." So even with
  full IaC, this step will likely still need a one-time CLI/console action (or a cluster import).
  This is a real reason the CLI deployer is pragmatic, and any IaC path must document it.

## Out of scope for these items (already done / no change needed)

- Core detection + self-heal logic, guardrails, regression tests, CI, live E2E — **done & validated**.
- L0 client hardening, docs, diagrams, bilingual README — **done**.
