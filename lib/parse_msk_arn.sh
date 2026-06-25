#!/usr/bin/env bash
# Single source of truth for parsing an MSK cluster ARN.
# arn:aws:kafka:REGION:ACCOUNT:cluster/NAME/UUID
# parse_msk_arn <arn> -> echoes: "<service> <region> <account> <clustername>"
# Returns non-zero (and echoes nothing) if the ARN is not a kafka cluster ARN.
parse_msk_arn() {
  local arn="$1" _a _aws _svc _region _acct _rest name
  IFS=':' read -r _a _aws _svc _region _acct _rest <<< "$arn"
  [[ "$_svc" != "kafka" ]] && return 1
  name="$(printf '%s' "$_rest" | sed -E 's#^cluster/([^/]+)/.*#\1#')"
  [[ -z "$name" || "$name" == "$_rest" ]] && return 1
  printf '%s %s %s %s\n' "$_svc" "$_region" "$_acct" "$name"
}
