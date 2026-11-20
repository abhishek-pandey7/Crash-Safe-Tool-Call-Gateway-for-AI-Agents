#!/usr/bin/env sh
# Walks through what the gateway does when responses are lost and when
# Postgres goes away, on the Docker Compose stack. Start it first:
#
#   docker compose up -d --build
#   ./scripts/demo.sh
#
# The fake tool server's ledger is the ground truth: it records every effect
# that really happened, and the gateway cannot see or change it.
set -eu
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1 # Git Bash on Windows: keep container paths as they are

# The same port variables as compose.yaml.
FAKE=http://127.0.0.1:${FAKETOOL_PORT:-8090}
TOXI=http://127.0.0.1:${TOXIPROXY_PORT:-8474}
GWA=http://127.0.0.1:${GATEWAY_A_PORT:-8080}
GWB=http://127.0.0.1:${GATEWAY_B_PORT:-8081}
run=$(date +%s) # fresh references on every run

# call URL TOOL ARGS [mcpcall flags...] runs one tools/call from inside the
# compose network, the way an agent would.
call() {
  url=$1 tool=$2 args=$3
  shift 3
  docker compose run --rm --no-deps -T --entrypoint /usr/local/bin/mcpcall gateway-a \
    -url "$url" -tool "$tool" -args "$args" "$@"
}
fault() { curl -fsS -X POST "$FAKE/faults" -H 'Content-Type: application/json' -d "$1" >/dev/null; }
# effects REF counts ledger entries whose top-level reference is REF. The
# same value also appears inside each entry's args, which is not counted.
effects() { curl -fsS "$FAKE/ledger" | grep -oE "\"reference\":\"$1\",\"(key|args)\"" | wc -l | tr -d ' '; }
toxi() { curl -fsS -X POST "$TOXI/proxies/$1" -H 'Content-Type: application/json' -d "{\"enabled\":$2}" >/dev/null; }
failed=0
# expect LABEL GOT WANT prints a count and records a mismatch.
expect() {
  if [ "$2" = "$3" ]; then
    echo "   $1: $2"
  else
    echo "   $1: $2, expected $3"
    failed=1
  fi
}
step() { printf '\n== %s\n' "$*"; }

for url in "$FAKE/requests" "$TOXI/proxies" "$GWA/-/readyz" "$GWB/-/readyz"; do
  curl -fsS "$url" >/dev/null || { echo "not ready: $url (run docker compose up -d --build)"; exit 1; }
done

ref="direct-$run"
step "1. No gateway: the refund's response is lost, and the agent retries"
fault '{"tool":"create_refund","dropResponse":true}'
call http://faketool:8080/mcp create_refund "{\"reference\":\"$ref\",\"amount\":20}" -retries 1 -timeout 5s
expect "refunds the tool really made" "$(effects "$ref")" 2

to="customer-$run@example.com"
step "2. Through the gateway: an email (no way to check or deduplicate) loses its response"
fault '{"tool":"send_email","dropResponse":true}'
call http://gateway-a:8080/mcp send_email "{\"to\":\"$to\",\"subject\":\"Your refund\"}" -key "email-$run"
echo "   retrying through the other gateway instance:"
call http://gateway-b:8080/mcp send_email "{\"to\":\"$to\",\"subject\":\"Your refund\"}" -key "email-$run"
expect "emails the tool really sent" "$(effects "$to")" 1

ref="refund-$run"
step "3. Through the gateway: a refund loses its response, and a read settles it"
fault '{"tool":"create_refund","dropResponse":true}'
call http://gateway-a:8080/mcp create_refund "{\"reference\":\"$ref\",\"amount\":20}" -key "$ref"
echo "   retrying through the other gateway instance:"
call http://gateway-b:8080/mcp create_refund "{\"reference\":\"$ref\",\"amount\":20}" -key "$ref"
expect "refunds the tool really made" "$(effects "$ref")" 1

ref="outage-$run"
step "4. Postgres is cut off: nothing is sent"
toxi postgres false
call http://gateway-a:8080/mcp create_refund "{\"reference\":\"$ref\",\"amount\":20}" -key "$ref"
expect "refunds the tool really made" "$(effects "$ref")" 0
toxi postgres true
sleep 3
echo "   Postgres is back; the same call again:"
call http://gateway-a:8080/mcp create_refund "{\"reference\":\"$ref\",\"amount\":20}" -key "$ref"
expect "refunds the tool really made" "$(effects "$ref")" 1

step "5. What gateway-a counted"
curl -fsS "$GWA/-/metrics" | grep -E '^gateway_(tool_calls_total|operations|store_up)' || true

if [ "$failed" -ne 0 ]; then
  echo
  echo "some counts did not match"
  exit 1
fi
