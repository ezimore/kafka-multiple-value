# MLA Performance Benchmark Suite

Compares three message routing architectures at high throughput (millions of events/sec):

| Option | Architecture | Filtering | Topics |
|--------|-------------|-----------|--------|
| **Option 1** | Client-side filtering | Each consumer filters by `event-id` header | 1 shared topic |
| **Option 2** | MLA broker-side filtering | Broker filters via authorization bitmap | 1 MLA-enabled topic |
| **Option 3** | Topic-per-audience | Producer routes to per-audience topics | 3 separate topics |

## Quick Start

```bash
# Build Kafka
./gradlew jar -x test -x spotbugsMain -x checkstyleMain

# Run the benchmark (all 3 options, 5M events each)
bash perf/mla-benchmark/run-benchmark.sh
```

Results are written to `perf/mla-benchmark/results/`.

## Architecture Details

### Option 1: Client-Side Filtering

```
Producer → [events topic] → Consumer1 (filters event-id=1)
                           → Consumer2 (filters event-id=1,2,3)
                           → Consumer3 (filters event-id=3,4)
```

- 1 topic, all events stored once
- Every consumer reads ALL records and discards non-matching ones
- Filtering CPU cost is on the consumer side
- Broker does no per-record work
- Consumers waste network bandwidth receiving records they discard

### Option 2: MLA Broker-Side Filtering

```
Producer → [events topic + mla-authz-bitmap] → Broker MLAPlugin → Consumer1 (gets event-id 1,2)
                                                                  → Consumer2 (gets event-id 1,2,3)
                                                                  → Consumer3 (gets event-id 3,4)
```

Authorization mapping:
| event-id | Authorized consumers |
|----------|---------------------|
| 1 | consumer1, consumer2 |
| 2 | consumer2 |
| 3 | consumer2, consumer3 |
| 4 | consumer3 |

- 1 topic, all events stored once
- Broker filters per-record during fetch (bitmap AND check)
- Consumers receive only authorized records — no wasted bandwidth
- Small overhead: bitmap header per record (~1 byte), broker CPU for bitwise check

### Option 3: Topic-Per-Audience Routing

```
Producer → [event1 topic] → Consumer1
         → [event2 topic] → Consumer2
         → [event3 topic] → Consumer3
```

Routing rules:
| event-id | Target topics |
|----------|--------------|
| 1 | event1, event2 |
| 2 | event2 |
| 3 | event2, event3 |
| 4 | event3 |

- 3 topics, some events duplicated across topics
- Producer sends each event to 1-2 topics (more produce calls, more network)
- Broker stores duplicate copies (more disk, more replication)
- Consumers read only their topic — no filtering needed
- Simplest consumer logic, highest storage cost

## What Gets Measured

For each component (producer, broker, consumers) and each option:

- **Throughput**: records/sec, MB/sec
- **Latency**: avg, p50, p95, p99, p99.9 (producer send latency)
- **CPU**: user + system time per process
- **Memory**: peak RSS, JVM heap usage
- **Disk I/O**: bytes written/read (broker log segments)
- **Storage**: total bytes on disk for topic data
- **Network**: bytes in/out per component

## Configuration

Edit `perf/mla-benchmark/benchmark.conf`:

```bash
NUM_EVENTS=5000000        # Total events to produce
EVENT_SIZE_MIN=200        # Min event payload bytes
EVENT_SIZE_MAX=300        # Max event payload bytes
NUM_EVENT_IDS=4           # Event ID values (1..N)
BROKER_HEAP=2g            # Broker JVM heap
PRODUCER_HEAP=1g          # Producer JVM heap
CONSUMER_HEAP=512m        # Consumer JVM heap
PARTITIONS=6              # Topic partitions
REPORT_INTERVAL_MS=5000   # Progress reporting interval
```


##Note, shall be deleted

The benchmark suite is ready. Here's what was created:

perf/mla-benchmark/ — complete performance benchmark comparing three architectures:

File	Purpose
README.md	Architecture details, quick start, what gets measured
benchmark.conf	Tunable parameters (event count, sizes, heap, partitions)
run-benchmark.sh	Orchestration script — starts broker, creates topics, runs producer/consumers, collects metrics, generates comparison report
BenchmarkProducer.java
Producer supporting all 3 options (client-side headers, MLA bitmaps, topic routing)
BenchmarkConsumer.java
Consumer supporting all 3 options (client-side filtering, MLA-filtered, topic-per-audience)
SetupRegistry.java
Creates the Consumer ID Registry and registers consumers for option 2
ResourceCollector.java
Collects CPU, memory (peak RSS), disk I/O, and storage per process via /proc
Metrics collected per component:

Throughput (records/sec, MB/sec)
Latency (avg, p50, p95, p99, p99.9) for the producer
CPU time (user + system seconds)
Peak RSS memory
Disk I/O (bytes read/written)
Total storage on disk
To run:

# All 3 options
bash perf/mla-benchmark/run-benchmark.sh

# Single option
bash perf/mla-benchmark/run-benchmark.sh 2
The final comparison report is written to 
comparison-report.txt
 with side-by-side metrics for all three architectures.