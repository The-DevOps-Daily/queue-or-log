#!/usr/bin/env bash
# Start one Kafka (KRaft, single node), one RabbitMQ and one NATS with JetStream,
# on the host network (ports 9092, 5672/15672 and 4222).
set -euo pipefail
cd "$(dirname "$0")"; source ./env.sh

$DOCKER run -d --name qol-kafka --network host \
  -e KAFKA_HEAP_OPTS="-Xmx512m -Xms512m" \
  "$KAFKA_IMAGE"
$DOCKER run -d --name qol-rabbit --network host "$RABBIT_IMAGE"
$DOCKER run -d --name qol-nats --network host "$NATS_IMAGE" -js

echo "waiting for the brokers..."
for _ in $(seq 1 90); do
  if $DOCKER exec qol-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 \
    && $DOCKER exec qol-rabbit rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
$DOCKER exec qol-kafka /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 describe | grep -E "share|group" || true
$DOCKER exec qol-rabbit rabbitmq-plugins enable rabbitmq_stream >/dev/null
echo "up"
