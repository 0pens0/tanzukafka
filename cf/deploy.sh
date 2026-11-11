#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CF_DIR="$ROOT_DIR/cf"
VARS_FILE="${VARS_FILE:-$CF_DIR/vars.yml}"
KAFKA_DIRECT="${KAFKA_DIRECT:-0}"

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
# Prefer explicit env overrides (useful in direct mode), else vars.yml, else defaults.
KAFKA_STREAMS_APP_ID="${KAFKA_STREAMS_APP_ID:-$(awk -F': ' '/^kafka_streams_app_id:/ {print $2}' "$VARS_FILE" | tr -d '"')}"
KAFKA_STREAMS_APP_ID="${KAFKA_STREAMS_APP_ID:-wordcount-v3}"
KAFKA_UI_CLUSTER_NAME="${KAFKA_UI_CLUSTER_NAME:-$(awk -F': ' '/^kafka_ui_cluster_name:/ {print $2}' "$VARS_FILE" | tr -d '"')}"
KAFKA_UI_CLUSTER_NAME="${KAFKA_UI_CLUSTER_NAME:-tanzu-kafka}"

echo "==> Building applications"
"$ROOT_DIR/scripts/build.sh"

push_marketplace() {
  if [[ -z "$KAFKA_SERVICE_NAME" || -z "$KAFKA_PLAN" ]]; then
    echo "kafka_service_name and kafka_plan must be set in $VARS_FILE" >&2
    exit 1
  fi

  echo "==> Ensuring Kafka service instance: $KAFKA_SERVICE_NAME"
  if ! cf service "$KAFKA_SERVICE_NAME" >/dev/null 2>&1; then
    if ! cf create-service kafka "$KAFKA_PLAN" "$KAFKA_SERVICE_NAME" 2>/dev/null; then
      echo "ERROR: Kafka marketplace service is not available in this space." >&2
      echo "       Set KAFKA_DIRECT=1 with TANZU_KAFKA_* env vars for hand-built Kafka," >&2
      echo "       or fix the marketplace broker, then re-run." >&2
      exit 1
    fi
    echo "Waiting for Kafka service to become available..."
    while true; do
      svc_status="$(cf service "$KAFKA_SERVICE_NAME" | awk '/status:/ {print $2, $3; exit}')"
      case "$svc_status" in
        "create succeeded"|"update succeeded") break ;;
        "create failed"|"update failed"|"delete failed")
          echo "ERROR: Kafka service entered failed state: $svc_status" >&2
          cf service "$KAFKA_SERVICE_NAME" >&2 || true
          exit 1
          ;;
      esac
      sleep 10
    done
  fi

  echo "==> Creating temporary service key for Kafka UI configuration"
  cf delete-service-key "$KAFKA_SERVICE_NAME" "$SERVICE_KEY_NAME" -f >/dev/null 2>&1 || true
  cf create-service-key "$KAFKA_SERVICE_NAME" "$SERVICE_KEY_NAME" >/dev/null

  echo "Waiting for service key credentials..."
  while true; do
    if CREDS_JSON="$(cf service-key "$KAFKA_SERVICE_NAME" "$SERVICE_KEY_NAME" 2>/dev/null \
        | sed -n '/^{/,$p' \
        | jq -c '.credentials // empty' 2>/dev/null)" \
      && [[ -n "$CREDS_JSON" && "$CREDS_JSON" != "null" ]]; then
      break
    fi
    sleep 5
  done

  BOOTSTRAP="$(echo "$CREDS_JSON" | jq -r '.bootstrap_servers[0]')"
  SECURITY_PROTOCOL="$(echo "$CREDS_JSON" | jq -r '.security_protocol')"
  SASL_MECHANISM="$(echo "$CREDS_JSON" | jq -r '.sasl.mechanism')"
  SASL_USERNAME="$(echo "$CREDS_JSON" | jq -r '.sasl.username')"
  SASL_PASSWORD="$(echo "$CREDS_JSON" | jq -r '.sasl.password')"
  if [[ -z "$BOOTSTRAP" || "$BOOTSTRAP" == "null" ]]; then
    echo "ERROR: could not read bootstrap_servers from service key" >&2
    exit 1
  fi
  JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${SASL_USERNAME}\" password=\"${SASL_PASSWORD}\";"

  echo "==> Deploying apps (marketplace) to $(cf target | tr '\n' ' ')"
  cf push -f "$CF_DIR/manifest.yml" \
    --vars-file "$VARS_FILE" \
    --var kafka_bootstrap_servers="$BOOTSTRAP" \
    --var kafka_security_protocol="$SECURITY_PROTOCOL" \
    --var kafka_sasl_mechanism="$SASL_MECHANISM" \
    --var kafka_sasl_jaas_config="$JAAS_CONFIG"
}

push_direct() {
  : "${TANZU_KAFKA_BOOTSTRAP_SERVERS:?set TANZU_KAFKA_BOOTSTRAP_SERVERS}"
  : "${TANZU_KAFKA_USERNAME:?set TANZU_KAFKA_USERNAME}"
  : "${TANZU_KAFKA_PASSWORD:?set TANZU_KAFKA_PASSWORD}"
  SECURITY_PROTOCOL="${TANZU_KAFKA_SECURITY_PROTOCOL:-SASL_SSL}"
  SASL_MECHANISM="${TANZU_KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}"
  JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${TANZU_KAFKA_USERNAME}\" password=\"${TANZU_KAFKA_PASSWORD}\";"

  CA_PEM_FILE="${KAFKA_CA_PEM_FILE:-$ROOT_DIR/kafka-cf-config/src/main/resources/opsmgr-services-tls-ca.pem}"
  if [[ ! -f "$CA_PEM_FILE" ]]; then
    echo "ERROR: CA PEM not found at $CA_PEM_FILE (set KAFKA_CA_PEM_FILE)" >&2
    exit 1
  fi

  TMP_VARS="$(mktemp)"
  chmod 600 "$TMP_VARS"
  cleanup() { rm -f "$TMP_VARS"; }
  trap cleanup EXIT

  LINES_TOPIC="${KAFKA_LINES_TOPIC:-$(awk -F': ' '/^kafka_lines_topic:/ {print $2}' "$VARS_FILE" | tr -d '"')}"
  COUNTS_TOPIC="${KAFKA_COUNTS_TOPIC:-$(awk -F': ' '/^kafka_counts_topic:/ {print $2}' "$VARS_FILE" | tr -d '"')}"
  LINES_TOPIC="${LINES_TOPIC:-lines}"
  COUNTS_TOPIC="${COUNTS_TOPIC:-counts-v2}"

  {
    echo "kafka_ui_cluster_name: \"${KAFKA_UI_CLUSTER_NAME}\""
    echo "kafka_streams_app_id: \"${KAFKA_STREAMS_APP_ID}\""
    echo "kafka_lines_topic: \"${LINES_TOPIC}\""
    echo "kafka_counts_topic: \"${COUNTS_TOPIC}\""
    echo "kafka_bootstrap_servers: \"${TANZU_KAFKA_BOOTSTRAP_SERVERS}\""
    echo "kafka_username: \"${TANZU_KAFKA_USERNAME}\""
    echo "kafka_password: \"${TANZU_KAFKA_PASSWORD}\""
    echo "kafka_security_protocol: \"${SECURITY_PROTOCOL}\""
    echo "kafka_sasl_mechanism: \"${SASL_MECHANISM}\""
    # YAML double-quoted string: escape \ and "
    printf 'kafka_sasl_jaas_config: "%s"\n' "$(printf '%s' "$JAAS_CONFIG" | sed 's/\\/\\\\/g; s/"/\\"/g')"
    echo "kafka_ca_pem: |"
    sed 's/^/  /' "$CA_PEM_FILE"
  } >"$TMP_VARS"

  echo "==> Deploying apps (direct / no marketplace) to $(cf target | tr '\n' ' ')"
  echo "    bootstrap=${TANZU_KAFKA_BOOTSTRAP_SERVERS} streams_id=${KAFKA_STREAMS_APP_ID}"
  cf push -f "$CF_DIR/manifest-direct.yml" --vars-file "$TMP_VARS"
}

if [[ "$KAFKA_DIRECT" == "1" || "$KAFKA_DIRECT" == "true" ]]; then
  push_direct
else
  push_marketplace
fi

echo
echo "Deployment complete."
echo "Word cloud UI: cf app tanzukafka-ui | awk '/routes:/ {print \$2}'"
echo "Kafka UI:      cf app tanzukafka-kafka-ui | awk '/routes:/ {print \$2}'"
echo
echo "Topics to inspect in Kafka UI: lines, counts"
