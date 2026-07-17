#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Building Tanzu Kafka demo apps..."
./mvnw -q clean package -DskipTests

echo "Build complete."
echo "  web-ui:           web-ui/target/web-ui-1.0.jar"
echo "  lines-generator:  lines-generator/target/lines-generator-1.0.jar"
echo "  wordcount-stream: wordcount-stream/target/wordcount-stream-1.0.jar"
