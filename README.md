## lamellae
lamellae is *YET ANOTHER* wait-free single-producer single-consumer queue (SPSC) implementation in Rust, built around a 
cache-line-partitioned ring buffer designed to minimize contention and false sharing between producer and consumer threads.

### Why the name?
A [*lamella*](https://en.wikipedia.org/wiki/Lamella_(materials)) (plural: *lamellae*) is a thin layer or plate that forms part of a 
larger structure. The name reflects lamellae's internal architecture, which partitions queue state into cache-line-sized regions, 
ensuring that producer and consumer state remain physically separated and cache-line exclusive.

### Core Model
Messages are written into cache-line-sized regions owned by the producer. A write does not immediately make a message visible to the 
consumer, instead, data remains buffered in the currently owned cache line. A cache line becomes visible to the consumer under two conditions:
- the cache line is fully filled
- the producer explicitly calls `flush()`

Once released, the consumer can read all messages contained within it. All that to say, visibility is defined at cache line granularity rather
than per message!

### When is lamellae a good fit?
- workloads with natural batching, where per-message visibility is not required
- high throughput pipelines where sustained throughput matters more than individual message latency
- bursty producers where cache-line amortization improves efficiency

### When is lamellae NOT a good fit?
- systems that require each message to be immediately visible to the consumer
- latency-sensitive pipelines where visibility delay is unacceptable
- APIs that assume per message queue semantics rather than buffered/cache-line windows

TLDR: if you want “send == immediate visible”, this is not that kind of queue and you should probably look at other queues like rtrb for example :p
