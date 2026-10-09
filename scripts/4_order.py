"""Experiment 4: ten events for one account, and event 3 fails once.

The events must apply in order (think balance updates). The handler fails the
first time it sees e3 and succeeds after that. One consumer reads, so any
reordering comes from the retry path alone. Retry timing varies, so every
setup runs five times. The per-message-ack setups run with several messages
in flight, and with exactly one.
"""
from brokers import (
    kafka_group_read, kafka_share_read, nats_bound_read, nats_consumer, nats_publish, nats_stream,
    rabbit_publish, rabbit_queue_read, rabbit_quorum, rabbit_stream, rabbit_stream_read,
)
from common import kafka_produce, kafka_topic, run_id, write_result

EVENTS = [f"e{i}" for i in range(1, 11)]
REPEAT = 5
results = {}


def flaky():
    seen = set()

    def handle(value, consumer=None):
        if value == "e3" and value not in seen:
            seen.add(value)
            raise RuntimeError("transient failure")
        return True

    return handle


def repeat(label, once):
    """Run one setup REPEAT times; once(rid) returns the reader's output."""
    orders = [once(run_id())["handled"] for _ in range(REPEAT)]
    results[label] = {
        "runs": REPEAT,
        "runs_in_order": sum(1 for o in orders if o == EVENTS),
        "orders": [" ".join(o) for o in orders],
    }


def kafka_group(rid):
    topic = f"order-group-{rid}"
    kafka_topic(topic)
    kafka_produce(topic, EVENTS, key=b"account-42")
    return kafka_group_read(topic, f"g-{rid}", flaky())


def kafka_share(max_poll_records):
    def once(rid):
        topic = f"order-share-{rid}"
        kafka_topic(topic)
        kafka_produce(topic, EVENTS, key=b"account-42")
        return kafka_share_read(topic, f"s-{rid}", flaky(), max_poll_records=max_poll_records)

    return once


def quorum(prefetch):
    def once(rid):
        q = f"order-quorum-{rid}"
        rabbit_quorum(q)
        rabbit_publish(q, EVENTS)
        return rabbit_queue_read(q, flaky(), prefetch=prefetch)

    return once


def stream(rid):
    s = f"order-stream-{rid}"
    rabbit_stream(s)
    rabbit_publish(s, EVENTS)
    return rabbit_stream_read(s, flaky())


def nats(kind, pending, batch):
    def once(rid):
        name = f"ORDER_{kind.upper()}_{rid}"
        nats_stream(name, kind)
        nats_publish(name, EVENTS)
        nats_consumer(name, "c", max_ack_pending=pending)
        return nats_bound_read(name, "c", flaky(), batch=batch)

    return once


repeat("kafka consumer group", kafka_group)
repeat("kafka share group (default)", kafka_share(None))
repeat("kafka share group (max.poll.records=1)", kafka_share(1))
repeat("rabbitmq quorum queue (prefetch 10)", quorum(10))
repeat("rabbitmq quorum queue (prefetch 1)", quorum(1))
repeat("rabbitmq stream", stream)
# NATS sends redeliveries before new messages, so with fetch(1) the failed
# event often comes straight back. Whether it does is a race, unless only one
# message may be in flight (max_ack_pending 1).
for kind in ("workqueue", "limits"):
    for pending, batch in ((1000, 10), (1000, 1), (1, 1)):
        repeat(f"nats {kind} stream (fetch {batch}, max_ack_pending {pending})", nats(kind, pending, batch))

write_result("4_order", {"events": " ".join(EVENTS), "fails_once": "e3", "results": results})
