#!/usr/bin/env bash
# End-to-end local verification: parses two source systems, then
# asserts acceptance/rejection/duplicate counts and replay determinism.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
INBOX="$ROOT/examples/inbox"
S1="$WORK/run1"; S2="$WORK/run2"
printf 'local-dev-pepper-do-not-use-in-prod' > "$WORK/secret.key"

echo "== go vet / tests (with race detector) =="
(cd "$ROOT" && go vet ./... && go test -race ./...)

run() {
  local state=$1
  (cd "$ROOT" && go run ./cmd/pipeline -inbox "$INBOX" -state "$state" \
    -secret "$WORK/secret.key" -aliases "$ROOT/examples/aliases.json" \
    -grace 48h -workers 4)
}

echo; echo "== first run =="
run "$S1" 2>&1 | tee "$WORK/first.log"
echo; echo "== replay in a fresh process (must be no-op) =="
run "$S1" 2>&1 | tee "$WORK/replay.log"
echo; echo "== independent second run against empty state =="
run "$S2" > "$WORK/second.log" 2>&1

check() { local want=$1; shift
  if ! "$@"; then echo "ASSERT FAILED: $want"; exit 1; fi
  echo "ok: $want"
}
echo; echo "== assertions =="
check "first run accepts 4 events, rejects 2" grep -q 'accepted=4 rejected=3 duplicates=1' "$WORK/first.log"
check "first run rejects with distinguishable reasons" \
  bash -c "grep -q INVALID_CODE '$WORK/first.log' && grep -q IDENTITY_CONFLICT '$WORK/first.log' && grep -q MALFORMED_MESSAGE '$WORK/first.log'"
check "identical redelivery recorded as duplicate" grep -q 'key=hospitalA|e1' "$WORK/first.log"
check "replay is idempotent no-op" grep -q 'accepted=0 rejected=0 duplicates=0' "$WORK/replay.log"
check "both runs yield identical event files" bash -c "diff -r '$S1/events' '$S2/events' >/dev/null"
check "both runs yield identical token maps" bash -c "cmp -s '$S1/token-map.json' '$S2/token-map.json'"
check "token map is access-controlled (0600)" bash -c "[ \$(stat -c %a '$S1/token-map.json') = 600 ]"
echo; echo "ALL LOCAL CHECKS PASSED"
