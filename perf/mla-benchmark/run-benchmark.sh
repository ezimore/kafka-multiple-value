#!/bin/bash
# =============================================================================
# MLA Performance Benchmark — Orchestration Script
#
# Compares three message routing architectures:
#   Option 1: Client-side filtering (consumers filter by event-id header)
#   Option 2: MLA broker-side filtering (broker filters via authorization bitmap)
#   Option 3: Topic-per-audience (producer routes to per-audience topics)
#
# Usage:
#   bash perf/mla-benchmark/run-benchmark.sh [option]
#
# If [option] is specified (1, 2, or 3), only that option is run.
# If omitted, all three options are run sequentially.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KAFKA_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load configuration
source "$SCRIPT_DIR/benchmark.conf"

# Derived paths
RESULTS_DIR="$KAFKA_HOME/$RESULTS_DIR"
CLASSPATH="$KAFKA_HOME/core/build/libs/*:$KAFKA_HOME/clients/build/libs/*:$KAFKA_HOME/server-common/build/libs/*:$KAFKA_HOME/server/build/libs/*:$KAFKA_HOME/tools/build/libs/*:$KAFKA_HOME/metadata/build/libs/*:$KAFKA_HOME/storage/build/libs/*:$KAFKA_HOME/storage/storage-api/build/libs/*:$KAFKA_HOME/raft/build/libs/*:$KAFKA_HOME/group-coordinator/group-coordinator-api/build/libs/*:$KAFKA_HOME/share-coordinator/build/libs/*:$KAFKA_HOME/transaction-coordinator/build/libs/*:$KAFKA_HOME/coordinator-common/build/libs/*:$KAFKA_HOME/tools/tools-api/build/libs/*"
PERF_CLASSPATH="$SCRIPT_DIR/build/classes:$CLASSPATH"
BOOTSTRAP="localhost:$BROKER_PORT"
SASL_BOOTSTRAP="localhost:$SASL_PORT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[BENCH]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
ok() { echo -e "${GREEN}[OK]${NC} $*"; }
err() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---- Build benchmark classes ----
build_benchmark() {
    log "Compiling benchmark classes..."
    mkdir -p "$SCRIPT_DIR/build/classes"

    # Find all Java source files
    SOURCES=$(find "$SCRIPT_DIR/src" -name "*.java")

    javac -cp "$CLASSPATH" \
        -d "$SCRIPT_DIR/build/classes" \
        $SOURCES

    ok "Benchmark classes compiled."
}

# ---- Broker management ----
start_broker() {
    local option=$1
    local config_file="$SCRIPT_DIR/build/server-option${option}.properties"

    log "Preparing broker config for option $option..."

    # Clean data directory
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    # Generate a cluster ID and format storage
    local cluster_id
    cluster_id=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)

    # Create broker config
    cp "$KAFKA_HOME/config/server.properties" "$config_file"

    # Override data directory
    sed -i "s|log.dirs=.*|log.dirs=$DATA_DIR|g" "$config_file"

    # Add MLA plugin config for option 2
    if [ "$option" -eq 2 ]; then
        echo "" >> "$config_file"
        echo "# MLA Plugin Configuration" >> "$config_file"
        echo "record.fetch.plugin.classes=org.apache.kafka.server.record.mla.MLAPlugin" >> "$config_file"
        echo "mla.strip.authorization.header=true" >> "$config_file"
        echo "mla.consumer.id.registry.topic=$REGISTRY_TOPIC" >> "$config_file"
        echo "" >> "$config_file"
        echo "# SASL/PLAIN listener for authenticated consumers" >> "$config_file"
        echo "listeners=PLAINTEXT://:${BROKER_PORT},CONTROLLER://:9093,SASL_PLAINTEXT://:${SASL_PORT}" >> "$config_file"
        echo "advertised.listeners=PLAINTEXT://localhost:${BROKER_PORT},CONTROLLER://localhost:9093,SASL_PLAINTEXT://localhost:${SASL_PORT}" >> "$config_file"
        echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT" >> "$config_file"
        echo "sasl.mechanism.inter.broker.protocol=PLAIN" >> "$config_file"
        echo "sasl.enabled.mechanisms=PLAIN" >> "$config_file"
        echo "listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\" >> "$config_file"
        echo "    username=\"admin\" \\" >> "$config_file"
        echo "    password=\"admin-secret\" \\" >> "$config_file"
        echo "    user_admin=\"admin-secret\" \\" >> "$config_file"
        echo "    user_${SASL_CONSUMER1_USER}=\"${SASL_CONSUMER1_PASS}\" \\" >> "$config_file"
        echo "    user_${SASL_CONSUMER2_USER}=\"${SASL_CONSUMER2_PASS}\" \\" >> "$config_file"
        echo "    user_${SASL_CONSUMER3_USER}=\"${SASL_CONSUMER3_PASS}\";" >> "$config_file"
    fi

    # Format storage
    "$KAFKA_HOME/bin/kafka-storage.sh" format \
        --config "$config_file" \
        --cluster-id "$cluster_id" \
        --standalone 2>/dev/null

    log "Starting broker (option $option)..."
    export KAFKA_HEAP_OPTS="-Xmx${BROKER_HEAP} -Xms${BROKER_HEAP}"
    "$KAFKA_HOME/bin/kafka-server-start.sh" "$config_file" \
        > "$RESULTS_DIR/broker-option${option}.log" 2>&1 &
    BROKER_PID=$!

    # Wait for broker to be ready
    log "Waiting for broker to start (PID $BROKER_PID)..."
    local retries=60
    while [ $retries -gt 0 ]; do
        if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; then
            ok "Broker started (PID $BROKER_PID)."
            return 0
        fi
        sleep 1
        retries=$((retries - 1))
    done

    err "Broker failed to start within 60 seconds."
    return 1
}

stop_broker() {
    if [ -n "${BROKER_PID:-}" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
        log "Stopping broker (PID $BROKER_PID)..."
        kill "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
        ok "Broker stopped."
    fi
}

# ---- Topic management ----
create_topics() {
    local option=$1

    case $option in
        1)
            log "Creating topic: events (no MLA)"
            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                --create --topic events --partitions "$PARTITIONS" \
                --replication-factor "$REPLICATION_FACTOR" 2>/dev/null
            ;;
        2)
            log "Creating registry topic and events topic (MLA-enabled)"
            # Registry topic (may already be auto-created by MLAPlugin, recreate with compaction)
            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                --create --topic "$REGISTRY_TOPIC" --partitions 1 \
                --replication-factor "$REPLICATION_FACTOR" \
                --config cleanup.policy=compact 2>/dev/null || true

            # Register consumers with real SASL principals
            java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.SetupRegistry \
                "$BOOTSTRAP" "$REGISTRY_TOPIC" \
                "User:${SASL_CONSUMER1_USER}" "User:${SASL_CONSUMER2_USER}" "User:${SASL_CONSUMER3_USER}"

            # Wait for MLAPlugin's background registry client to pick up registrations
            log "Waiting for MLAPlugin registry client to sync..."
            sleep 10

            # Events topic with MLA plugin
            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                --create --topic events --partitions "$PARTITIONS" \
                --replication-factor "$REPLICATION_FACTOR" \
                --config record.fetch.plugins=org.apache.kafka.server.record.mla.MLAPlugin 2>/dev/null
            ;;
        3)
            log "Creating topics: event1, event2, event3"
            for t in event1 event2 event3; do
                "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                    --create --topic "$t" --partitions "$PARTITIONS" \
                    --replication-factor "$REPLICATION_FACTOR" 2>/dev/null
            done
            ;;
    esac

    ok "Topics created for option $option."
}

# ---- Run a single option ----
run_option() {
    local option=$1
    local option_dir="$RESULTS_DIR/option${option}"
    mkdir -p "$option_dir"

    echo ""
    log "============================================"
    log "  RUNNING OPTION $option"
    log "============================================"
    echo ""

    # Start broker
    start_broker "$option"

    # Create topics
    create_topics "$option"

    # Start resource collector for broker
    java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
        "$BROKER_PID" "broker" "$option_dir/resource-broker.txt" 1000 300 &
    BROKER_RESOURCE_PID=$!

    # Start consumers in background
    local consumer_pids=()

    case $option in
        1)
            # Option 1: 3 consumers with client-side filtering
            # consumer1 filters for event-id 1
            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                1 1 "$BOOTSTRAP" "events" "1" 30 "$REPORT_INTERVAL_MS" \
                > "$option_dir/consumer1.log" 2>&1 &
            consumer_pids+=($!)

            # consumer2 filters for event-id 1,2,3
            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                1 2 "$BOOTSTRAP" "events" "1,2,3" 30 "$REPORT_INTERVAL_MS" \
                > "$option_dir/consumer2.log" 2>&1 &
            consumer_pids+=($!)

            # consumer3 filters for event-id 3,4
            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                1 3 "$BOOTSTRAP" "events" "3,4" 30 "$REPORT_INTERVAL_MS" \
                > "$option_dir/consumer3.log" 2>&1 &
            consumer_pids+=($!)
            ;;
        2)
            # Option 2: 3 consumers with SASL authentication, broker filters via MLA
            # Each consumer authenticates as a different user so the MLAPlugin can
            # distinguish them and apply per-consumer bitmap filtering.
            local sasl_bootstrap="localhost:${SASL_PORT}"

            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                2 1 "$sasl_bootstrap" "events" "none" 60 "$REPORT_INTERVAL_MS" \
                "$SASL_CONSUMER1_USER" "$SASL_CONSUMER1_PASS" \
                > "$option_dir/consumer1.log" 2>&1 &
            consumer_pids+=($!)

            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                2 2 "$sasl_bootstrap" "events" "none" 60 "$REPORT_INTERVAL_MS" \
                "$SASL_CONSUMER2_USER" "$SASL_CONSUMER2_PASS" \
                > "$option_dir/consumer2.log" 2>&1 &
            consumer_pids+=($!)

            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                2 3 "$sasl_bootstrap" "events" "none" 60 "$REPORT_INTERVAL_MS" \
                "$SASL_CONSUMER3_USER" "$SASL_CONSUMER3_PASS" \
                > "$option_dir/consumer3.log" 2>&1 &
            consumer_pids+=($!)
            ;;
        3)
            # Option 3: 3 consumers, each on its own topic
            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                3 1 "$BOOTSTRAP" "event1" "none" 30 "$REPORT_INTERVAL_MS" \
                > "$option_dir/consumer1.log" 2>&1 &
            consumer_pids+=($!)

            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                3 2 "$BOOTSTRAP" "event2" "none" 30 "$REPORT_INTERVAL_MS" \
                > "$option_dir/consumer2.log" 2>&1 &
            consumer_pids+=($!)

            java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                org.apache.kafka.perf.mla.BenchmarkConsumer \
                3 3 "$BOOTSTRAP" "event3" "none" 30 "$REPORT_INTERVAL_MS" \
                > "$option_dir/consumer3.log" 2>&1 &
            consumer_pids+=($!)
            ;;
    esac

    # Start resource collectors for consumers
    sleep 2  # Let consumers start
    local consumer_resource_pids=()
    for i in "${!consumer_pids[@]}"; do
        local cpid=${consumer_pids[$i]}
        local cnum=$((i + 1))
        if kill -0 "$cpid" 2>/dev/null; then
            java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
                "$cpid" "consumer${cnum}" "$option_dir/resource-consumer${cnum}.txt" 1000 300 &
            consumer_resource_pids+=($!)
        fi
    done

    # Run producer
    log "Starting producer for option $option..."
    java -Xmx${PRODUCER_HEAP} -cp "$PERF_CLASSPATH" \
        org.apache.kafka.perf.mla.BenchmarkProducer \
        "$option" "$BOOTSTRAP" "$NUM_EVENTS" "$EVENT_SIZE_MIN" "$EVENT_SIZE_MAX" \
        "$NUM_EVENT_IDS" "$PRODUCER_BATCH_SIZE" "$PRODUCER_LINGER_MS" "$REPORT_INTERVAL_MS" \
        > "$option_dir/producer.log" 2>&1

    ok "Producer finished for option $option."

    # Wait for consumers to drain (they have a 30-second timeout)
    log "Waiting for consumers to finish consuming..."
    for cpid in "${consumer_pids[@]}"; do
        wait "$cpid" 2>/dev/null || true
    done
    ok "All consumers finished for option $option."

    # Collect disk usage
    java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
        disk "$DATA_DIR" "$option_dir/resource-disk.txt"

    # Stop resource collectors
    for rpid in "$BROKER_RESOURCE_PID" "${consumer_resource_pids[@]}"; do
        kill "$rpid" 2>/dev/null || true
        wait "$rpid" 2>/dev/null || true
    done

    # Stop broker
    stop_broker

    ok "Option $option complete. Results in $option_dir/"
}

# ---- Generate comparison report ----
generate_report() {
    local report="$RESULTS_DIR/comparison-report.txt"

    echo "============================================================" > "$report"
    echo "  MLA PERFORMANCE BENCHMARK — COMPARISON REPORT" >> "$report"
    echo "  $(date)" >> "$report"
    echo "  Events: $NUM_EVENTS, Size: ${EVENT_SIZE_MIN}-${EVENT_SIZE_MAX} bytes" >> "$report"
    echo "============================================================" >> "$report"
    echo "" >> "$report"

    for opt in 1 2 3; do
        local dir="$RESULTS_DIR/option${opt}"
        if [ ! -d "$dir" ]; then
            continue
        fi

        echo "------------------------------------------------------------" >> "$report"
        case $opt in
            1) echo "  OPTION 1: Client-Side Filtering" >> "$report" ;;
            2) echo "  OPTION 2: MLA Broker-Side Filtering" >> "$report" ;;
            3) echo "  OPTION 3: Topic-Per-Audience Routing" >> "$report" ;;
        esac
        echo "------------------------------------------------------------" >> "$report"
        echo "" >> "$report"

        # Producer results
        if [ -f "$dir/producer.log" ]; then
            echo "  PRODUCER:" >> "$report"
            grep -E "^(total_records|total_bytes|elapsed_sec|records_per_sec|mb_per_sec|avg_latency|max_latency|p50|p95|p99|errors)" \
                "$dir/producer.log" | sed 's/^/    /' >> "$report"
            echo "" >> "$report"
        fi

        # Consumer results
        for cid in 1 2 3; do
            if [ -f "$dir/consumer${cid}.log" ]; then
                echo "  CONSUMER $cid:" >> "$report"
                grep -E "^(total_records|total_bytes|elapsed_sec|accepted_records_per_sec|mb_per_sec)" \
                    "$dir/consumer${cid}.log" | sed 's/^/    /' >> "$report"
                echo "" >> "$report"
            fi
        done

        # Resource usage
        echo "  RESOURCE USAGE:" >> "$report"
        for component in broker consumer1 consumer2 consumer3; do
            if [ -f "$dir/resource-${component}.txt" ]; then
                echo "    $component:" >> "$report"
                cat "$dir/resource-${component}.txt" | sed 's/^/      /' >> "$report"
            fi
        done

        # Disk usage
        if [ -f "$dir/resource-disk.txt" ]; then
            echo "    disk:" >> "$report"
            cat "$dir/resource-disk.txt" | sed 's/^/      /' >> "$report"
        fi

        echo "" >> "$report"
    done

    echo "============================================================" >> "$report"
    echo "  END OF REPORT" >> "$report"
    echo "============================================================" >> "$report"

    ok "Comparison report: $report"
    echo ""
    cat "$report"
}

# ---- Main ----
main() {
    local run_options="${1:-all}"

    log "MLA Performance Benchmark"
    log "Events: $NUM_EVENTS, Size: ${EVENT_SIZE_MIN}-${EVENT_SIZE_MAX} bytes"
    log "Broker heap: $BROKER_HEAP, Producer heap: $PRODUCER_HEAP, Consumer heap: $CONSUMER_HEAP"
    echo ""

    # Build
    build_benchmark

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    # Cleanup handler
    trap 'stop_broker; kill 0 2>/dev/null || true' EXIT

    if [ "$run_options" = "all" ]; then
        run_option 1
        run_option 2
        run_option 3
        generate_report
    else
        run_option "$run_options"
        generate_report
    fi
}

main "$@"
