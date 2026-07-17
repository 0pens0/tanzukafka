#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CF_DIR="$ROOT_DIR/cf"
VARS_FILE="${VARS_FILE:-$CF_DIR/vars.yml}"

if ! command -v cf >/dev/null 2>&1; then
  echo "cf CLI is required" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

KAFKA_SERVICE_NAME="$(awk -F': ' '/^kafka_service_name:/ {print $2}' "$VARS_FILE" | tr -d '"')"
KAFKA_PLAN="$(awk -F': ' '/^kafka_plan:/ {print $2}' "$VARS_FILE" | tr -d '"')"
KAFKA_UI_CLUSTER_NAME="$(awk -F': ' '/^kafka_ui_cluster_name:/ {print $2}' "$VARS_FILE" | tr -d '"')"
SERVICE_KEY_NAME="${SERVICE_KEY_NAME:-tanzukafka-deploy-key}"

if [[ -z "$KAFKA_SERVICE_NAME" || -z "$KAFKA_PLAN" ]]; then
  echo "kafka_service_name and kafka_plan must be set in $VARS_FILE" >&2
  exit 1
fi

echo "==> Building applications"
"$ROOT_DIR/scripts/build.sh"

echo "==> Ensuring Kafka service instance: $KAFKA_SERVICE_NAME"
if ! cf service "$KAFKA_SERVICE_NAME" >/dev/null 2>&1; then
  cf create-service kafka "$KAFKA_PLAN" "$KAFKA_SERVICE_NAME"
  echo "Waiting for Kafka service to become available..."
  while true; do
  status="$(cf service "$KAFKA_SERVICE_NAME" | awk '/^status:/ {print $2}')"
  if [[ "$status" == "create" ]]; then
    sleep 10
    continue
  fi
  break
  done
fi

echo "==> Creating temporary service key for Kafka UI configuration"
cf delete-service-key "$KAFKA_SERVICE_NAME" "$SERVICE_KEY_NAME" -f >/dev/null 2>&1 || true
cf create-service-key "$KAFKA_SERVICE_NAME" "$SERVICE_KEY_NAME" >/dev/null

CREDS_JSON="$(cf service-key "$KAFKA_SERVICE_NAME" "$SERVICE_KEY_NAME" | awk 'found {print} /credentials/ {found=1}' | tail -n +2 | sed '$d')"
BOOTSTRAP="$(echo "$CREDS_JSON" | jq -r '.credentials.bootstrap_servers[0]')"
SECURITY_PROTOCOL="$(echo "$CREDS_JSON" | jq -r '.credentials.security_protocol')"
SASL_MECHANISM="$(echo "$CREDS_JSON" | jq -r '.credentials.sasl.mechanism')"
SASL_USERNAME="$(echo "$CREDS_JSON" | jq -r '.credentials.sasl.username')"
SASL_PASSWORD="$(echo "$CREDS_JSON" | jq -r '.credentials.sasl.password')"
JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${SASL_USERNAME}\" password=\"${SASL_PASSWORD}\";"

echo "==> Deploying apps to $(cf target | tr '\n' ' ')"
cf push -f "$CF_DIR/manifest.yml" \
  --vars-file "$VARS_FILE" \
  --var kafka_bootstrap_servers="$BOOTSTRAP" \
  --var kafka_security_protocol="$SECURITY_PROTOCOL" \
  --var kafka_sasl_mechanism="$SASL_MECHANISM" \
  --var kafka_sasl_jaas_config="$JAAS_CONFIG"

echo
echo "Deployment complete."
echo "Word cloud UI: cf app tanzukafka-ui | awk '/urls:/ {print \$2}'"
echo "Kafka UI:      cf app tanzukafka-kafka-ui | awk '/urls:/ {print \$2}'"
echo
echo "Topics to inspect in Kafka UI: lines, counts"
