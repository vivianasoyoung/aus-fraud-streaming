# What I'd do differently in production

This is a demo. Real bank-scale fraud detection would differ in several
important ways. Listing them here both as honesty and as a starting point
for the inevitable "what would you change?" interview question.

## Velocity tracking
- **Demo:** Per-consumer in-memory deque per account_id.
- **Production:** Redis with TTL'd sorted sets, or stateful stream processing
  (Kafka Streams / Flink / ksqlDB) so state survives restarts and scales
  horizontally. Keys are partitioned by account_id, but state per account
  still needs to be checkpointed.

## Scoring model
- **Demo:** Hand-coded rule weights summed together.
- **Production:** Rules generate features; a supervised model (gradient
  boosted trees, LightGBM) ingests those features alongside graph features
  (device, IP, payee network) and historical embeddings. Calibration
  matters — score 80 should mean 80% chance of fraud.

## Decisioning
- **Demo:** Single threshold, write to one table.
- **Production:** Three-tier — allow / step-up auth / block — with each tier
  emitting a separate Kafka topic that downstream systems (auth service,
  customer comms, ops queue) subscribe to.

## Schema registry
- **Demo:** Raw JSON, Pydantic validates on consume.
- **Production:** Avro or Protobuf with Confluent Schema Registry. Producers
  fail at publish time if they break the contract. Consumers get
  forward/backward compatibility guarantees.

## Exactly-once
- **Demo:** Idempotent producer + at-least-once consumer + UNIQUE constraint.
- **Production:** Kafka transactions on the producer + read_committed isolation
  on the consumer + transactional DB writes — true exactly-once across the
  pipeline.

## Observability
- **Demo:** stdout logging.
- **Production:** Structured JSON logs → OpenSearch, metrics (lag,
  rule-firing rates, score distribution) → Prometheus / Grafana,
  per-message tracing → OpenTelemetry. Alerts on lag > 30s and on rule
  firing rates drifting >2σ from baseline.

## Backpressure & SLAs
- **Demo:** Single consumer, no SLA.
- **Production:** Multiple consumer instances in the group, partition count
  sized for peak throughput, p99 latency budget (e.g. "score within 200ms
  of card swipe").

## Compliance
- **Demo:** No PII handling.
- **Production:** PII tokenization at ingress, audit log of every decision
  with reason codes (AUSTRAC / banking regulator requirements), GDPR-style
  delete pipelines, encryption in flight and at rest.
