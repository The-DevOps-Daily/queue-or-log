"""Experiment 2b: the RabbitMQ nack result, on two RabbitMQ versions.

RabbitMQ 4.3 changed which requeues count towards a quorum queue's delivery
limit. This runs the same poison message test (limit 5, a dead letter queue,
basic.nack and then basic.reject) against whichever RabbitMQ RABBIT_URL points
at, and records the server version with the result.

    ../.venv/bin/python 2b_nack_by_version.py 4.3.6
"""
import sys
import time
from collections import Counter

from brokers import rabbit_channel, rabbit_publish, rabbit_queue_read, rabbit_quorum
from common import run_id, write_result

N, POISON, BUDGET = 100, "10", 30.0
label = sys.argv[1]
rid = run_id()
conn, ch = rabbit_channel()
server = conn._impl.server_properties.get("version", b"?")
server = server.decode() if isinstance(server, bytes) else str(server)
conn.close()

results = {}
for fail_with in ("nack", "reject"):
    q, dlq = f"v-{fail_with}-{rid}", f"v-dead-{fail_with}-{rid}"
    rabbit_quorum(q, delivery_limit=5, dead_letter=dlq)
    rabbit_publish(q, range(N))
    tries = Counter()

    def handle(value, consumer=None):
        if value == POISON:
            tries[value] += 1
            raise RuntimeError("cannot process")
        return True

    out = rabbit_queue_read(q, handle, deadline=BUDGET, fail_with=fail_with)
    time.sleep(2)
    conn, ch = rabbit_channel()
    dead = ch.queue_declare(dlq, passive=True).method.message_count
    left = ch.queue_declare(q, passive=True).method.message_count
    conn.close()
    results[f"basic.{fail_with}"] = {
        "others_handled": len({v for v in out["handled"] if v != POISON}),
        "poison_attempts": tries[POISON],
        "dead_letter_count": dead,
        "messages_left": left,
    }

write_result(f"2b_nack_rabbitmq_{label}", {"rabbitmq_version": server, "delivery_limit": 5, "budget_seconds": BUDGET, "results": results})
