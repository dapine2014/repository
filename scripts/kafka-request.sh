#!/usr/bin/env bash
set -euo pipefail

REQUEST_FILE=${1:-}
BROKER=${2:-localhost:29092}
REQUEST_TOPIC=${3:-REPOSITORY_QUERY}
RESPONSE_TOPIC=${4:-REPOSITORY_RESPONSE}
TIMEOUT_SECONDS=${5:-12}

if [[ -z "$REQUEST_FILE" ]]; then
  echo "Usage: $0 <request.json> [broker] [request_topic] [response_topic] [timeout_seconds]" >&2
  exit 1
fi

if [[ ! -f "$REQUEST_FILE" ]]; then
  echo "Request file not found: $REQUEST_FILE" >&2
  exit 1
fi

timeout "${TIMEOUT_SECONDS}s" docker run --rm --network host confluentinc/cp-kafka:7.4.4 \
  kafka-console-consumer --bootstrap-server "$BROKER" \
  --topic "$RESPONSE_TOPIC" --from-beginning --timeout-ms 10000 &

CONSUMER_PID=$!
sleep 1

python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)))" < "$REQUEST_FILE" | \
  docker run --rm --network host -i confluentinc/cp-kafka:7.4.4 \
  kafka-console-producer --bootstrap-server "$BROKER" --topic "$REQUEST_TOPIC"

wait $CONSUMER_PID
