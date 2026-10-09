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

## What the runs said

- **Replay:** held. It worked on every setup whose storage keeps handled messages (both Kafka setups, the RabbitMQ stream, the NATS limits stream) and returned nothing on the two that delete them.
- **Poison:** held, with one exception we did not expect. The two offset readers stopped at the poison message, and the per-message setups moved it aside, except the RabbitMQ 4.3 quorum queue with `basic.nack`, which retried it in a loop. RabbitMQ 4.3 stopped counting nacks towards the delivery limit; 4.2.9 still counts them (`runs/2b_*`).
- **Parallelism:** the consumer group used one consumer per partition, as claimed. The Kafka share group did too, which the claim did not predict: a member acquires whole producer batches. With one record per batch it used all six.
- **Order:** wrong as stated. Per-message acknowledgements kept order in every run with exactly one message in flight (RabbitMQ prefetch 1, NATS `max_ack_pending` 1). They lost it in every run where a consumer held several messages, and in 3 of 5 NATS runs with one message per fetch but many allowed in flight.
