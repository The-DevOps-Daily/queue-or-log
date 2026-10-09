# Shared settings for the scripts. Point DOCKER at your docker CLI if it is not on PATH.
export DOCKER="${DOCKER:-docker}"
export KAFKA_IMAGE="apache/kafka:4.3.1"
export RABBIT_IMAGE="rabbitmq:4.3.6-management"
export NATS_IMAGE="nats:2.15.0"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export RABBIT_URL="${RABBIT_URL:-amqp://guest:guest@localhost:5672/%2F}"
export NATS_URL="${NATS_URL:-nats://localhost:4222}"
