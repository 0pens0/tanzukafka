# tanzukafka

Kafka word cloud demo for **Tanzu Platform**, packaged with **[Kafka UI](https://github.com/provectus/kafka-ui)** for topic inspection.

Based on [Tanzu-Solutions-Engineering/word_cloud-spring-kafka-on-k8s](https://github.com/Tanzu-Solutions-Engineering/word_cloud-spring-kafka-on-k8s), adapted for:

- **Tanzu for Apache Kafka** marketplace service bindings (`VCAP_SERVICES`)
- **Cloud Foundry** multi-app deployment (no Kubernetes, no GemFire/PCC)
- **Kafka UI** alongside the demo apps

## Architecture

```
lines-generator  ──►  lines topic  ──►  wordcount-stream  ──►  counts topic
                                                              │
                                                              ├──► web-ui (word cloud)
                                                              └──► Kafka UI (inspect topics)
```

| App | Route | Purpose |
|-----|-------|---------|
| `tanzukafka-ui` | yes | Vue/Vuetify word cloud dashboard |
| `tanzukafka-lines` | no | Scrapes spring.io blog text → `lines` topic |
| `tanzukafka-stream` | no | Kafka Streams word count → `counts` topic |
| `tanzukafka-kafka-ui` | yes | Kafka UI for topics/messages |

## Prerequisites

- Tanzu Platform with **kafka** in `cf marketplace` **or** a reachable Kafka cluster plus credentials for direct mode
- `cf` CLI, `jq`, Java 8+ for local builds
- Docker support on the foundation (for Kafka UI app)

## Local build

```bash
./scripts/build.sh
```

For local Kafka testing, use the upstream `kafka-local` pattern or any Kafka on `localhost:9092`. The shared `kafka-cf-config` module only activates when `VCAP_SERVICES` is present.

Run apps (separate terminals):

```bash
java -jar lines-generator/target/lines-generator-1.0.jar
java -jar wordcount-stream/target/wordcount-stream-1.0.jar
java -jar web-ui/target/web-ui-1.0.jar
```

Open http://localhost:8084

## Deploy to Tanzu Platform

### Marketplace Kafka (preferred)

1. Edit `cf/vars.yml` — set `kafka_service_name` and `kafka_plan` for your marketplace.
2. `cf target` your org/space.
3. Run:

```bash
chmod +x cf/deploy.sh scripts/build.sh
./cf/deploy.sh
```

`deploy.sh` will:

- Build all Spring Boot jars
- Create the Kafka service instance if missing
- Read credentials via a service key for Kafka UI env vars
- `cf push` all four apps from `cf/manifest.yml`

### Direct Kafka (no marketplace broker)

Homelab Strimzi / TCP-routed Kafka without an OSB offering:

```bash
export KAFKA_DIRECT=1
export TANZU_KAFKA_BOOTSTRAP_SERVERS='broker.kafka.tcp.example:9092'
export TANZU_KAFKA_USERNAME='...'
export TANZU_KAFKA_PASSWORD='...'
export TANZU_KAFKA_SECURITY_PROTOCOL=SASL_SSL
export TANZU_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
# optional: KAFKA_CA_PEM_FILE=/path/to/ca.pem
./cf/deploy.sh
```

Uses `cf/manifest-direct.yml` (no `services:` binding). Bump `kafka_streams_app_id` / `kafka_counts_topic` in `cf/vars.yml` to reset word counts.

**Gotcha:** do not set `SPRING_CLOUD_STREAM_BINDINGS_LINES_IN_DESTINATION` via plain CF env — underscores are relaxed-bound as nested `lines.in`. Use `SPRING_APPLICATION_JSON` (as in `manifest-direct.yml`) or keep destinations in `application.yaml` only.

## What changed from upstream

- Removed **cache-server** and **GemFire/PCC** — web-ui now consumes the `counts` topic directly
- Removed **windowed-wordcount-stream** for a simpler v1 demo
- Added **kafka-cf-config** — auto-configures Spring Cloud Stream from `VCAP_SERVICES`
- Added **cf/** manifests and **kafka-ui** Docker app packaging

## Demo flow

1. Open **word cloud UI** — watch words appear as blog text is processed
2. Open **Kafka UI** — browse `lines` and `counts` topics, consumer groups
3. Show service binding — all Java apps bound to the same marketplace Kafka instance

## License

Upstream workshop code; see original repository for attribution.
