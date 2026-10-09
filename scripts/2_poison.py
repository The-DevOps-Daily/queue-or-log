"""Experiment 2: one message can never be processed.

100 messages go to one partition, queue or stream. Message "10" makes the
handler fail every time. Each setup gets 45 seconds, using its own failure
path. Recorded: how many of the other 99 were handled, how often the poison
message was tried, and where it ended up.
"""
import time
from collections import Counter

import pika

from brokers import (
    in_threads, kafka_group_read, kafka_read_offset, kafka_share_read, nats_watch,
    nats_publish, nats_read, nats_stream, nats_stream_count,
    rabbit_channel, rabbit_publish, rabbit_queue_read, rabbit_quorum, rabbit_stream, rabbit_stream_read,
)
from common import kafka_produce, kafka_topic, run_id, write_result

N, POISON, BUDGET = 100, "10", 45.0
rid = run_id()
results = {}


def poison_handler():
    tries = Counter()

    def handle(value, consumer=None):
        if value == POISON:
            tries[value] += 1
            raise RuntimeError("cannot process")
        return True

    return handle, tries


def summary(out, tries, elapsed, **extra):
    others = [v for v in out["handled"] if v != POISON]
    return {"others_handled": len(set(others)), "poison_attempts": tries[POISON], "seconds": round(elapsed, 1), **extra}


def timed(fn):
    t0 = time.time()
    out = fn()
    return out, time.time() - t0


# Kafka consumer group: seek back and retry in place
topic = f"poison-group-{rid}"
kafka_topic(topic)
kafka_produce(topic, range(N))
h, tries = poison_handler()
out, el = timed(lambda: kafka_group_read(topic, f"g-{rid}", h, deadline=BUDGET))
results["kafka consumer group"] = summary(out, tries, el, poison_ends="still the next offset; the partition does not move past it")

# Kafka share group: release, redelivered until the delivery count limit
topic = f"poison-share-{rid}"
kafka_topic(topic)
kafka_produce(topic, range(N))
h, tries = poison_handler()
out, el = timed(lambda: kafka_share_read(topic, f"s-{rid}", h, deadline=BUDGET))
still_there = kafka_read_offset(topic, 0, int(POISON))
results["kafka share group"] = summary(
    out, tries, el, poison_max_delivery_count=out["max_delivery_count"].get(POISON), record_at_offset_10_afterwards=still_there,
    poison_ends="archived by the share group: no member gets it again; it is still in the topic at its offset",
)

# RabbitMQ quorum queue, defaults (no dead letter exchange)
q = f"poison-quorum-{rid}"
rabbit_quorum(q)
rabbit_publish(q, range(N))
h, tries = poison_handler()
out, el = timed(lambda: rabbit_queue_read(q, h, deadline=BUDGET))
time.sleep(2)  # let the unacked delivery return to the queue before counting
conn, ch = rabbit_channel()
left = ch.queue_declare(q, passive=True).method.message_count
conn.close()
results["rabbitmq quorum queue (defaults, basic.nack)"] = summary(out, tries, el, messages_left=left, poison_ends="dropped after the default delivery limit" if left == 0 else "still in the queue, retried in a loop")

# RabbitMQ quorum queue, delivery limit 5 and a dead letter queue, failing with
# basic.nack and then with basic.reject: on 4.x only reject counts towards the limit.
for fail_with in ("nack", "reject"):
    q, dlq = f"poison-quorum-dlx-{fail_with}-{rid}", f"poison-dead-{fail_with}-{rid}"
    rabbit_quorum(q, delivery_limit=5, dead_letter=dlq)
    rabbit_publish(q, range(N))
    h, tries = poison_handler()
    out, el = timed(lambda: rabbit_queue_read(q, h, deadline=BUDGET, fail_with=fail_with))
    time.sleep(2)  # let the unacked delivery return to the queue before counting
    conn, ch = rabbit_channel()
    dead = ch.queue_declare(dlq, passive=True).method.message_count
    left = ch.queue_declare(q, passive=True).method.message_count
    method, props, body = ch.basic_get(dlq, auto_ack=False)
    conn.close()
    ends = "in the dead letter queue" if dead else "still in the queue, retried in a loop"
    results[f"rabbitmq quorum queue (limit 5 + dead letter, basic.{fail_with})"] = summary(
        out, tries, el, dead_letter_count=dead, dead_letter_body=body.decode() if body else None, messages_left=left, poison_ends=ends
    )

# RabbitMQ stream: resubscribe at the failed offset
s = f"poison-stream-{rid}"
rabbit_stream(s)
rabbit_publish(s, range(N))
h, tries = poison_handler()
out, el = timed(lambda: rabbit_stream_read(s, h, deadline=BUDGET))
results["rabbitmq stream"] = summary(out, tries, el, poison_ends="still the next offset; the reader does not move past it")

# NATS work-queue stream, max_deliver 5
name = f"POISON_WQ_{rid}"
nats_stream(name, "workqueue")
nats_publish(name, range(N))
h, tries = poison_handler()
(out, el), advisories = in_threads([
    lambda: timed(lambda: nats_read(name, "c", h, deadline=BUDGET, idle=8.0)),
    lambda: nats_watch(f"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{name}.>", 15),
])
results["nats work-queue stream (max_deliver 5)"] = summary(
    out, tries, el, messages_left_in_stream=nats_stream_count(name), max_deliveries_advisories=len(advisories),
    poison_ends="left in the stream, never delivered to this consumer again",
)

# NATS limits stream, max_deliver 5
name = f"POISON_LIM_{rid}"
nats_stream(name, "limits")
nats_publish(name, range(N))
h, tries = poison_handler()
(out, el), advisories = in_threads([
    lambda: timed(lambda: nats_read(name, "c", h, deadline=BUDGET, idle=8.0)),
    lambda: nats_watch(f"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{name}.>", 15),
])
results["nats limits stream (max_deliver 5)"] = summary(
    out, tries, el, messages_left_in_stream=nats_stream_count(name), max_deliveries_advisories=len(advisories),
    poison_ends="kept in the stream with the rest; this consumer stops delivering it",
)

write_result("2_poison", {"messages": N, "poison": POISON, "budget_seconds": BUDGET, "results": results})
