"""The six consumption setups, each with the same small API.

Each setup reads messages and hands them to a handler. The handler returns
True for success or raises for failure; what happens on failure is the
broker's own mechanism for that setup, configured as its documentation
recommends. Nothing here retries in application code unless the setup is an
offset reader, where retrying in place is the only option short of skipping.
"""
import asyncio
import threading
import time

import pika
from confluent_kafka import AcknowledgeType, Consumer, ShareConsumer, TopicPartition

from common import KAFKA, NATS, RABBIT, kafka_tool, share_group_config


# ---------------------------------------------------------------- Kafka


def kafka_group_read(topic, group, handler, idle=6.0, deadline=60.0, start=None):
    """Consumer group: one offset per partition, committed after each message.

    On a handler failure the consumer seeks back to the failed offset and
    tries again, which is what at-least-once processing with ordering means
    for an offset reader. `start` = "earliest" resets the group first.
    """
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    handled, attempts, last, t0 = [], 0, None, time.time()
    # The idle clock starts at the first message: joining a group takes a few seconds.
    while time.time() - t0 < deadline and (last is None or time.time() - last < idle):
        msg = c.poll(0.5)
        if msg is None or msg.error():
            continue
        last = time.time()
        attempts += 1
        try:
            handler(msg.value().decode(), consumer=group)
        except Exception:
            c.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
            continue
        handled.append(msg.value().decode())
        c.commit(message=msg, asynchronous=False)
    c.close()
    return {"handled": handled, "attempts": attempts}


def kafka_share_read(topic, group, handler, idle=8.0, deadline=90.0, member="m", max_poll_records=None):
    """Share group: each record is acquired, then accepted or released.

    A failure releases the record, so the broker redelivers it, to any member,
    until its delivery count reaches the group limit; then it is archived.
    """
    share_group_config(group, share_auto_offset_reset="earliest")
    cfg = {"bootstrap.servers": KAFKA, "group.id": group, "share.acknowledgement.mode": "explicit"}
    if max_poll_records:
        cfg["max.poll.records"] = max_poll_records
    c = ShareConsumer(cfg)
    c.subscribe([topic])
    handled, attempts, deliveries, last, t0 = [], 0, {}, None, time.time()
    while time.time() - t0 < deadline and (last is None or time.time() - last < idle):
        batch = c.poll(0.5)
        got = False
        for msg in batch:
            if msg.error():
                continue
            got = True
            attempts += 1
            value = msg.value().decode()
            deliveries[value] = max(deliveries.get(value, 0), msg.delivery_count())
            try:
                handler(value, consumer=member)
            except Exception:
                c.acknowledge(msg, AcknowledgeType.RELEASE)
                continue
            c.acknowledge(msg, AcknowledgeType.ACCEPT)
            handled.append(value)
        if got:
            c.commit_sync()
            last = time.time()
    c.close()
    return {"handled": handled, "attempts": attempts, "max_delivery_count": deliveries}


def kafka_share_reset(group, topic):
    """Rewind a share group to the start of the topic. The group must be empty."""
    return kafka_tool("kafka-share-groups.sh", "--reset-offsets", "--group", group, "--topic", topic, "--to-earliest", "--execute")


# ---------------------------------------------------------------- RabbitMQ


def rabbit_channel():
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT))
    return conn, conn.channel()


def rabbit_quorum(name, delivery_limit=None, dead_letter=None):
    conn, ch = rabbit_channel()
    args = {"x-queue-type": "quorum"}
    if delivery_limit is not None:
        args["x-delivery-limit"] = delivery_limit
    if dead_letter:
        ch.queue_declare(dead_letter, durable=True, arguments={"x-queue-type": "quorum"})
        args["x-dead-letter-exchange"] = ""
        args["x-dead-letter-routing-key"] = dead_letter
    ch.queue_declare(name, durable=True, arguments=args)
    conn.close()


def rabbit_stream(name):
    conn, ch = rabbit_channel()
    ch.queue_declare(name, durable=True, arguments={"x-queue-type": "stream"})
    conn.close()


def rabbit_publish(name, values):
    conn, ch = rabbit_channel()
    ch.confirm_delivery()
    for v in values:
        ch.basic_publish("", name, str(v).encode(), pika.BasicProperties(delivery_mode=2))
    conn.close()


def rabbit_queue_read(name, handler, idle=4.0, deadline=60.0, member="m", prefetch=10, fail_with="nack"):
    """Quorum queue: competing consumers; a failure goes back to the queue.

    fail_with="nack" uses basic.nack(requeue=True), "reject" basic.reject(requeue=True).
    On RabbitMQ 4.x only the reject counts towards the queue's delivery limit.
    """
    conn, ch = rabbit_channel()
    ch.basic_qos(prefetch_count=prefetch)
    handled, attempts, t0 = [], 0, time.time()
    for method, props, body in ch.consume(name, inactivity_timeout=idle):
        if method is None or time.time() - t0 > deadline:
            break
        attempts += 1
        value = body.decode()
        try:
            handler(value, consumer=member)
        except Exception:
            if fail_with == "reject":
                ch.basic_reject(method.delivery_tag, requeue=True)
            else:
                ch.basic_nack(method.delivery_tag, requeue=True)
            continue
        ch.basic_ack(method.delivery_tag)
        handled.append(value)
    ch.cancel()
    conn.close()
    return {"handled": handled, "attempts": attempts}


def rabbit_stream_read(name, handler, offset="first", idle=4.0, deadline=60.0, member="m"):
    """Stream: every consumer reads from an offset; acks are only flow control.

    A stream cannot redeliver one message, so on failure the reader resubscribes
    from the failed offset, the offset reader's only way to retry in order.
    """
    handled, attempts, t0 = [], 0, time.time()
    next_offset = offset
    while time.time() - t0 < deadline:
        conn, ch = rabbit_channel()
        ch.basic_qos(prefetch_count=100)
        failed = False
        for method, props, body in ch.consume(name, inactivity_timeout=idle, arguments={"x-stream-offset": next_offset}):
            if method is None or time.time() - t0 > deadline:
                break
            attempts += 1
            value = body.decode()
            stream_offset = props.headers["x-stream-offset"]
            try:
                handler(value, consumer=member)
            except Exception:
                next_offset, failed = stream_offset, True
                break
            ch.basic_ack(method.delivery_tag)
            handled.append(value)
        ch.cancel()
        conn.close()
        if not failed:
            break
    return {"handled": handled, "attempts": attempts}


# ---------------------------------------------------------------- NATS


async def _nats():
    import nats

    nc = await nats.connect(NATS)
    return nc, nc.jetstream()


def nats_stream(name, retention):
    """retention: "workqueue" (deleted once acked) or "limits" (kept until limits)."""
    from nats.js.api import RetentionPolicy, StreamConfig

    async def go():
        nc, js = await _nats()
        policy = RetentionPolicy.WORK_QUEUE if retention == "workqueue" else RetentionPolicy.LIMITS
        await js.add_stream(StreamConfig(name=name, subjects=[f"{name}.>"], retention=policy))
        await nc.close()

    asyncio.run(go())


def nats_publish(name, values, subject_fn=None):
    async def go():
        nc, js = await _nats()
        for i, v in enumerate(values):
            await js.publish(subject_fn(i) if subject_fn else f"{name}.jobs", str(v).encode())
        await nc.close()

    asyncio.run(go())


def nats_stream_count(name):
    async def go():
        nc, js = await _nats()
        info = await js.stream_info(name)
        await nc.close()
        return info.state.messages

    return asyncio.run(go())


def nats_read(name, durable, handler, workers=1, idle=4.0, deadline=60.0, max_deliver=5, max_ack_pending=1000, ack_wait=2.0):
    """Durable pull consumer with explicit acks, shared by `workers` fetchers.

    A failure is nak'ed, so the server redelivers it until max_deliver.
    """
    from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

    async def go():
        nc, js = await _nats()
        cfg = ConsumerConfig(
            durable_name=durable,
            ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=DeliverPolicy.ALL,
            max_deliver=max_deliver,
            max_ack_pending=max_ack_pending,
            ack_wait=ack_wait,
        )
        await js.add_consumer(name, cfg)
        handled, attempts, deliveries = [], [0], {}
        t0 = time.time()

        async def worker(member):
            sub = await js.pull_subscribe_bind(durable, stream=name)
            last = time.time()
            while time.time() - t0 < deadline and time.time() - last < idle:
                try:
                    msgs = await sub.fetch(1, timeout=1)
                except Exception:
                    continue
                for m in msgs:
                    last = time.time()
                    attempts[0] += 1
                    value = m.data.decode()
                    deliveries[value] = max(deliveries.get(value, 0), m.metadata.num_delivered)
                    try:
                        handler(value, consumer=member)
                    except Exception:
                        await m.nak()
                        continue
                    await m.ack_sync()
                    handled.append(value)

        await asyncio.gather(*(worker(f"w{i}") for i in range(workers)))
        await nc.close()
        return {"handled": handled, "attempts": attempts[0], "max_delivery_count": deliveries}

    return asyncio.run(go())


# ---------------------------------------------------------------- helpers


def in_threads(fns):
    """Run each callable in its own thread and return their results in order."""
    out = [None] * len(fns)

    def run(i, fn):
        out[i] = fn()

    threads = [threading.Thread(target=run, args=(i, fn)) for i, fn in enumerate(fns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return out


def nats_consumer(name, durable, max_deliver=5, max_ack_pending=1000, ack_wait=2.0):
    """Create a durable pull consumer once, so several readers can share it."""
    from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

    async def go():
        nc, js = await _nats()
        await js.add_consumer(name, ConsumerConfig(
            durable_name=durable, ack_policy=AckPolicy.EXPLICIT, deliver_policy=DeliverPolicy.ALL,
            max_deliver=max_deliver, max_ack_pending=max_ack_pending, ack_wait=ack_wait,
        ))
        await nc.close()

    asyncio.run(go())


def nats_bound_read(name, durable, handler, member="m", idle=4.0, deadline=60.0, batch=1):
    """One reader on an existing durable consumer, in its own event loop (one per thread)."""

    async def go():
        nc, js = await _nats()
        sub = await js.pull_subscribe_bind(durable, stream=name)
        handled, attempts, t0, last = [], 0, time.time(), None
        while time.time() - t0 < deadline and (last is None or time.time() - last < idle):
            try:
                msgs = await sub.fetch(batch, timeout=1)
            except Exception:
                continue
            for m in msgs:
                last = time.time()
                attempts += 1
                value = m.data.decode()
                try:
                    handler(value, consumer=member)
                except Exception:
                    await m.nak()
                    continue
                await m.ack_sync()
                handled.append(value)
        await nc.close()
        return {"handled": handled, "attempts": attempts}

    return asyncio.run(go())


def nats_watch(subject, seconds):
    """Collect messages on a core NATS subject for a while (used for advisories)."""

    async def go():
        nc, _ = await _nats()
        seen = []

        async def on_msg(msg):
            seen.append(msg.subject)

        await nc.subscribe(subject, cb=on_msg)
        await asyncio.sleep(seconds)
        await nc.close()
        return seen

    return asyncio.run(go())


def kafka_read_offset(topic, partition, offset):
    """Read one record by offset with a plain consumer, outside any group."""
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "offset-peek", "enable.auto.commit": False})
    c.assign([TopicPartition(topic, partition, offset)])
    value = None
    for _ in range(20):
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            value = msg.value().decode()
            break
    c.close()
    return value
