"""Experiment 1: a bug shipped, so the last 1,000 messages need processing again.

Each setup handles 1,000 messages, then tries to read them all a second time
the way that setup allows: reset the consumer group, rewind the share group,
read the stream from the first offset, or start a new consumer.
"""
from brokers import (
    kafka_group_read, kafka_share_read, kafka_share_reset,
    nats_publish, nats_read, nats_stream, nats_stream_count,
    rabbit_publish, rabbit_queue_read, rabbit_quorum, rabbit_stream, rabbit_stream_read,
)
from common import kafka_produce, kafka_tool, kafka_topic, run_id, write_result

N = 1000
ok = lambda value, consumer=None: True  # noqa: E731
rid = run_id()
results = {}

# Kafka, consumer group (offset reader on a log)
topic = f"replay-group-{rid}"
kafka_topic(topic)
kafka_produce(topic, range(N))
first = kafka_group_read(topic, f"g-{rid}", ok)
kafka_tool("kafka-consumer-groups.sh", "--reset-offsets", "--group", f"g-{rid}", "--topic", topic, "--to-earliest", "--execute")
second = kafka_group_read(topic, f"g-{rid}", ok, deadline=30.0)
results["kafka consumer group"] = {"storage": "log", "consumption": "offset", "first": len(first["handled"]), "replayed": len(second["handled"]), "how": "kafka-consumer-groups.sh --reset-offsets --to-earliest"}

# Kafka, share group (per-message acks on a log)
topic = f"replay-share-{rid}"
kafka_topic(topic)
kafka_produce(topic, range(N))
first = kafka_share_read(topic, f"s-{rid}", ok)
again = kafka_share_read(topic, f"s-{rid}", ok, deadline=20.0)
kafka_share_reset(f"s-{rid}", topic)
second = kafka_share_read(topic, f"s-{rid}", ok)
results["kafka share group"] = {"storage": "log", "consumption": "per-message ack", "first": len(first["handled"]), "same_group_again": len(again["handled"]), "replayed": len(second["handled"]), "how": "kafka-share-groups.sh --reset-offsets --to-earliest"}

# RabbitMQ, quorum queue (per-message acks, deleted once acked)
q = f"replay-quorum-{rid}"
rabbit_quorum(q)
rabbit_publish(q, range(N))
first = rabbit_queue_read(q, ok)
second = rabbit_queue_read(q, ok)
results["rabbitmq quorum queue"] = {"storage": "queue", "consumption": "per-message ack", "first": len(first["handled"]), "replayed": len(second["handled"]), "how": "consume again; there is nothing to rewind"}

# RabbitMQ, stream (offset reader on a log)
s = f"replay-stream-{rid}"
rabbit_stream(s)
rabbit_publish(s, range(N))
first = rabbit_stream_read(s, ok)
second = rabbit_stream_read(s, ok, offset="first")
results["rabbitmq stream"] = {"storage": "log", "consumption": "offset", "first": len(first["handled"]), "replayed": len(second["handled"]), "how": "a new consumer with x-stream-offset=first"}

# NATS JetStream, work-queue stream (per-message acks, deleted once acked)
name = f"REPLAY_WQ_{rid}"
nats_stream(name, "workqueue")
nats_publish(name, range(N))
first = nats_read(name, "first", ok)
left = nats_stream_count(name)
results["nats work-queue stream"] = {"storage": "queue", "consumption": "per-message ack", "first": len(first["handled"]), "messages_left_in_stream": left, "replayed": 0, "how": "nothing to rewind: the stream deletes each acked message"}

# NATS JetStream, limits stream (per-message acks, kept until limits)
name = f"REPLAY_LIM_{rid}"
nats_stream(name, "limits")
nats_publish(name, range(N))
first = nats_read(name, "first", ok)
second = nats_read(name, "replay", ok)
results["nats limits stream"] = {"storage": "log", "consumption": "per-message ack", "first": len(first["handled"]), "messages_left_in_stream": nats_stream_count(name), "replayed": len(second["handled"]), "how": "a new durable consumer with deliver_policy=all"}

write_result("1_replay", {"messages": N, "results": results})
