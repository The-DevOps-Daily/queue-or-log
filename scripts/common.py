"""Shared helpers: broker addresses, run ids, admin calls and result files."""
import json
import os
import shlex
import subprocess
import time
import uuid
from pathlib import Path

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
RABBIT = os.environ.get("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
NATS = os.environ.get("NATS_URL", "nats://localhost:4222")
DOCKER = shlex.split(os.environ.get("DOCKER", "docker"))
RUNS = Path(__file__).resolve().parent.parent / "runs"


def run_id() -> str:
    """A short suffix so every run uses fresh topics, queues, streams and groups."""
    return uuid.uuid4().hex[:6]


def kafka_tool(tool: str, *args: str) -> str:
    """Run one of Kafka's own CLI tools inside the broker container."""
    cmd = [*DOCKER, "exec", "qol-kafka", f"/opt/kafka/bin/{tool}", "--bootstrap-server", "localhost:9092", *args]
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def kafka_topic(name: str, partitions: int = 1) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA})
    admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[name].result(30)


def kafka_produce(topic: str, values, key=None, partition=None, one_record_batches=False) -> None:
    """Produce values. By default the producer batches records, as producers do;
    one_record_batches=True sends every record in a batch of its own."""
    from confluent_kafka import Producer

    cfg = {"bootstrap.servers": KAFKA, "linger.ms": 5}
    if one_record_batches:
        cfg.update({"linger.ms": 0, "batch.num.messages": 1})
    p = Producer(cfg)
    for i, v in enumerate(values):
        k = key(i) if callable(key) else key
        kwargs = {"key": k} if k is not None else {}
        if partition is not None:
            kwargs["partition"] = partition(i) if callable(partition) else partition
        p.produce(topic, value=str(v).encode(), **kwargs)
        if one_record_batches:
            p.flush(30)
    p.flush(30)


def share_group_config(group: str, **settings: str) -> None:
    """Set share group configs such as share.auto.offset.reset=earliest."""
    pairs = ",".join(f"{k.replace('_', '.')}={v}" for k, v in settings.items())
    kafka_tool("kafka-configs.sh", "--alter", "--entity-type", "groups", "--entity-name", group, "--add-config", pairs)


def write_result(name: str, data: dict) -> Path:
    RUNS.mkdir(exist_ok=True)
    path = RUNS / f"{name}.json"
    data = {"recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **data}
    path.write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps(data, indent=2))
    return path
