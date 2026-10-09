"""Turn the recorded runs into the tables in the README."""
import json
from pathlib import Path

RUNS = Path(__file__).resolve().parent.parent / "runs"


def load(name):
    return json.loads((RUNS / f"{name}.json").read_text())["results"]


def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


replay = load("1_replay")
print("## 1. Replay 1,000 handled messages\n")
print(table(["Setup", "Storage", "Consumption", "Read again"], [[k, v["storage"], v["consumption"], v["replayed"]] for k, v in replay.items()]))

poison = load("2_poison")
print("\n## 2. One poison message among 100 (45 s budget)\n")
print(table(["Setup", "Other 99 handled", "Poison attempts", "Where the poison ended"], [[k, v["others_handled"], v["poison_attempts"], v["poison_ends"]] for k, v in poison.items()]))

par = load("3_parallel")
print("\n## 3. Six consumers, 120 messages of 50 ms each\n")
print(table(["Setup", "Messages per consumer", "Busy", "Handles", "First to last handle (s)"], [[k, " ".join(map(str, v["per_consumer"])), v["busy_consumers"], v["total_handles"], v["seconds_first_to_last_handle"]] for k, v in par.items()]))

order = load("4_order")
print("\n## 4. Ten events, e3 fails once (5 runs each)\n")
print(table(["Setup", "Runs in order", "Order seen"], [[k, f"{v['runs_in_order']}/{v['runs']}", " / ".join(sorted(set(v["orders"])))] for k, v in order.items()]))
