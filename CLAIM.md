# The claim, written before the experiments ran

Every broker tested here (Kafka 4.3, RabbitMQ 4.3, NATS 2.15) can now act as a
queue and as a log, so "which broker" is the wrong first question. Two
separate choices decide the behaviour:

1. **Storage:** does the broker keep a message after it was handled?
2. **Consumption:** does a consumer acknowledge each message, or keep an offset?

The claim, per experiment:

| Experiment | Per-message acks | Offset |
| --- | --- | --- |
| Replay after a bad deploy | possible only if storage keeps handled messages | possible |
| One poison message | retried a few times, then moved aside; the rest flows | that partition stops until code skips it |
| More consumers than partitions | every consumer gets work | consumers past the partition count sit idle |
| Order per key after one retry | broken | kept |

What would prove it wrong:

- A per-message-ack mode that keeps per-key order after a retry.
- An offset mode that moves past a poison message without application code
  deciding to skip it.
- A Kafka consumer group that gives work to more consumers than partitions.
- Replay working on storage that deletes handled messages, or failing on
  storage that keeps them.
