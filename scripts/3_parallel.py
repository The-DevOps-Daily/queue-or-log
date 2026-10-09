"""Experiment 3: six consumers, but the Kafka topic has three partitions.

120 messages, each taking 50 ms to handle, so one consumer needs about 6
seconds and six consumers about 1. Recorded per setup: how many messages each
of the six consumers handled, how many handles happened in total, and the
wall time to drain.
"""
import time

from brokers import (
    in_threads, kafka_group_read, kafka_share_read, nats_bound_read, nats_consumer, nats_publish, nats_stream,
    rabbit_publish, rabbit_queue_read, rabbit_quorum, rabbit_stream, rabbit_stream_read,
)
from common import kafka_produce, kafka_topic, run_id, write_result

N, CONSUMERS, PARTITIONS, WORK = 120, 6, 3, 0.05
rid = run_id()
results = {}


stamps = []


def slow(value, consumer=None):
    time.sleep(WORK)
    stamps.append(time.time())
    return True


def report(outs, elapsed, **extra):
    per = [len(o["handled"]) for o in outs]
    return {"per_consumer": per, "busy_consumers": sum(1 for p in per if p), "total_handles": sum(per), "distinct_messages": len({v for o in outs for v in o["handled"]}), "seconds_first_to_last_handle": round(elapsed, 2), **extra}


def drain(fns):
    """Run the readers; the time is first handle to last handle, so the
    readers' join time and their idle wait at the end do not count."""
    stamps.clear()
    in_threads_out = in_threads(fns)
    elapsed = (max(stamps) - min(stamps) + WORK) if stamps else 0.0
    return in_threads_out, elapsed


# Kafka consumer group on 3 partitions
topic = f"par-group-{rid}"
kafka_topic(topic, PARTITIONS)
kafka_produce(topic, range(N), partition=lambda i: i % PARTITIONS)
outs, el = drain([lambda: kafka_group_read(topic, f"g-{rid}", slow, idle=8.0) for _ in range(CONSUMERS)])
results["kafka consumer group"] = report(outs, el, partitions=PARTITIONS)

# Kafka share group on the same shape of topic
topic = f"par-share-{rid}"
kafka_topic(topic, PARTITIONS)
kafka_produce(topic, range(N), partition=lambda i: i % PARTITIONS)
outs, el = drain([lambda i=i: kafka_share_read(topic, f"s-{rid}", slow, member=f"m{i}") for i in range(CONSUMERS)])
results["kafka share group"] = report(outs, el, partitions=PARTITIONS, max_poll_records="default (500)")

# Kafka share group again, with each member taking one record per poll. In the
# default share.acquire.mode (batch_optimized) max.poll.records is only a soft
# cap: a member acquires whole producer batches. librdkafka 2.15 has no
# share.acquire.mode setting, so record_limit cannot be tried from Python.
topic = f"par-share1-{rid}"
kafka_topic(topic, PARTITIONS)
kafka_produce(topic, range(N), partition=lambda i: i % PARTITIONS)
outs, el = drain([lambda i=i: kafka_share_read(topic, f"s1-{rid}", slow, member=f"m{i}", max_poll_records=1) for i in range(CONSUMERS)])
results["kafka share group (max.poll.records=1)"] = report(outs, el, partitions=PARTITIONS)

# And with the producer sending every record in its own batch
topic = f"par-share-single-{rid}"
kafka_topic(topic, PARTITIONS)
kafka_produce(topic, range(N), partition=lambda i: i % PARTITIONS, one_record_batches=True)
outs, el = drain([lambda i=i: kafka_share_read(topic, f"s2-{rid}", slow, member=f"m{i}", max_poll_records=1) for i in range(CONSUMERS)])
results["kafka share group (one record per producer batch)"] = report(outs, el, partitions=PARTITIONS)

# RabbitMQ quorum queue, six competing consumers, prefetch 1
q = f"par-quorum-{rid}"
rabbit_quorum(q)
rabbit_publish(q, range(N))
outs, el = drain([lambda i=i: rabbit_queue_read(q, slow, member=f"m{i}", prefetch=1) for i in range(CONSUMERS)])
results["rabbitmq quorum queue"] = report(outs, el)

# RabbitMQ stream, six consumers: each one reads the whole stream
s = f"par-stream-{rid}"
rabbit_stream(s)
rabbit_publish(s, range(N))
outs, el = drain([lambda i=i: rabbit_stream_read(s, slow, member=f"m{i}") for i in range(CONSUMERS)])
results["rabbitmq stream"] = report(outs, el, note="every consumer of a stream reads every message")

# NATS work-queue stream, one durable consumer shared by six readers
name = f"PAR_WQ_{rid}"
nats_stream(name, "workqueue")
nats_publish(name, range(N))
nats_consumer(name, "c")
outs, el = drain([lambda i=i: nats_bound_read(name, "c", slow, member=f"m{i}") for i in range(CONSUMERS)])
results["nats work-queue stream"] = report(outs, el)

# NATS limits stream, the same
name = f"PAR_LIM_{rid}"
nats_stream(name, "limits")
nats_publish(name, range(N))
nats_consumer(name, "c")
outs, el = drain([lambda i=i: nats_bound_read(name, "c", slow, member=f"m{i}") for i in range(CONSUMERS)])
results["nats limits stream"] = report(outs, el)

write_result("3_parallel", {"messages": N, "consumers": CONSUMERS, "seconds_per_message": WORK, "results": results})
