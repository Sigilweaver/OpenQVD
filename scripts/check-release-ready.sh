#!/usr/bin/env bash
# Refuse to say a commit is release-ready unless the most recent CI and
# audit workflow runs for it both completed successfully.
#
# Usage: scripts/check-release-ready.sh [ref]
#   ref defaults to HEAD. Any git-rev-parse-able ref works (branch, tag,
#   short/long SHA).
#
# Exits 0 and prints a confirmation if ci.yml and audit.yml both have a
# completed, successful run for the resolved commit. Exits 1 with an
# explanation otherwise. Requires the gh CLI to be authenticated
# (`gh auth status`).

set -euo pipefail

REF="${1:-HEAD}"

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh CLI not found; install/authenticate GitHub CLI to run this check" >&2
  exit 1
fi

SHA="$(git rev-parse "$REF")"
echo "Checking release readiness for ${REF} (${SHA})"

check_workflow() {
  local workflow="$1"
  local run_json
  run_json="$(gh run list -w "$workflow" -c "$SHA" --json status,conclusion,url -L 1)"

  if [ "$(echo "$run_json" | jq 'length')" -eq 0 ]; then
    echo "FAIL: no run of ${workflow} found for commit ${SHA}"
    return 1
  fi

  local status conclusion url
  status="$(echo "$run_json" | jq -r '.[0].status')"
  conclusion="$(echo "$run_json" | jq -r '.[0].conclusion')"
  url="$(echo "$run_json" | jq -r '.[0].url')"

  if [ "$status" != "completed" ]; then
    echo "FAIL: latest ${workflow} run for ${SHA} has status '${status}' (not completed) - ${url}"
    return 1
  fi

  if [ "$conclusion" != "success" ]; then
    echo "FAIL: latest ${workflow} run for ${SHA} concluded '${conclusion}' (not success) - ${url}"
    return 1
  fi

  echo "OK: ${workflow} passed for ${SHA} - ${url}"
  return 0
}

ci_ok=0
audit_ok=0
check_workflow ci.yml || ci_ok=1
check_workflow audit.yml || audit_ok=1

if [ "$ci_ok" -ne 0 ] || [ "$audit_ok" -ne 0 ]; then
  exit 1
fi

echo "Release ready: ci.yml and audit.yml are both green for ${SHA}."
exit 0
