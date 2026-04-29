#!/bin/bash
# =============================================================================
# MLA Performance Benchmark — Orchestration Script
#
# Compares four message routing architectures:
#   Option 1:  Client-side filtering (consumers filter by event-id header)
#   Option 2a: MLA broker-side filtering, header stripping ENABLED
#   Option 2b: MLA broker-side filtering, header stripping DISABLED
#   Option 3:  Topic-per-audience (producer routes to per-consumer topics)
#
# Usage:
#   bash perf/mla-benchmark/run-benchmark.sh [--nr-events N] [0|1|2a|2b|3]
# =============================================================================

set -euo pipefail
trap 'echo "[ERROR] Script failed at line $LINENO (exit code $?)" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KAFKA_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/benchmark.conf"

RESULTS_DIR="$KAFKA_HOME/$RESULTS_DIR"
CLASSPATH="$KAFKA_HOME/core/build/libs/*:$KAFKA_HOME/core/build/dependant-libs-2.13.18/*:$KAFKA_HOME/clients/build/libs/*:$KAFKA_HOME/server-common/build/libs/*:$KAFKA_HOME/server/build/libs/*:$KAFKA_HOME/tools/build/libs/*:$KAFKA_HOME/metadata/build/libs/*:$KAFKA_HOME/storage/build/libs/*:$KAFKA_HOME/storage/storage-api/build/libs/*:$KAFKA_HOME/raft/build/libs/*:$KAFKA_HOME/group-coordinator/group-coordinator-api/build/libs/*:$KAFKA_HOME/share-coordinator/build/libs/*:$KAFKA_HOME/transaction-coordinator/build/libs/*:$KAFKA_HOME/coordinator-common/build/libs/*:$KAFKA_HOME/tools/tools-api/build/libs/*"
PERF_CLASSPATH="$SCRIPT_DIR/build/classes:$CLASSPATH"
BOOTSTRAP="localhost:$BROKER_PORT"
SASL_BOOTSTRAP="localhost:$SASL_PORT"

# Parse CONSUMER_PCTS into arrays
OLD_IFS="$IFS"
IFS=',' read -ra PCT_ARRAY <<< "$CONSUMER_PCTS"
IFS="$OLD_IFS"
NUM_CONSUMERS=${#PCT_ARRAY[@]}

# Build consumer names like consumer-1p, consumer-5p, etc.
CONSUMER_NAMES=()
CONSUMER_USERS=()
CONSUMER_PASSES=()
for pct in "${PCT_ARRAY[@]}"; do
    pct=$(echo "$pct" | tr -d ' ')
    CONSUMER_NAMES+=("consumer-${pct}p")
    CONSUMER_USERS+=("user${pct}p")
    CONSUMER_PASSES+=("pass${pct}p")
done

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log() { echo -e "${BLUE}[BENCH]${NC} $*"; }
ok()  { echo -e "${GREEN}[OK]${NC} $*"; }
err() { echo -e "${RED}[ERROR]${NC} $*"; }

fmt_num() { echo "$1" | sed ':a;s/\B[0-9]\{3\}\>/ &/;ta'; }

# ---- Build ----
build_benchmark() {
    log "Compiling benchmark classes..."
    mkdir -p "$SCRIPT_DIR/build/classes"
    javac -cp "$CLASSPATH" -d "$SCRIPT_DIR/build/classes" \
        $(find "$SCRIPT_DIR/src" -name "*.java")
    ok "Benchmark classes compiled."
}

# ---- Broker ----
start_broker() {
    local option=$1
    local config_file="$SCRIPT_DIR/build/server-option${option}.properties"

    rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"
    local cluster_id=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
    cp "$KAFKA_HOME/config/server.properties" "$config_file"
    sed -i "s|log.dirs=.*|log.dirs=$DATA_DIR|g" "$config_file"

    # Remove any existing MLA/SASL config from the base file (may be left from manual testing)
    sed -i \
        -e '/^record\.fetch\.plugin\.classes/d' \
        -e '/^mla\./d' \
        -e '/^auto\.create\.topics\.enable/d' \
        -e '/^sasl\./d' \
        -e '/^listener\.name\.sasl/d' \
        -e '/^security\.inter\.broker/d' \
        -e '/^[[:space:]]*username=/d' \
        -e '/^[[:space:]]*password=/d' \
        -e '/^[[:space:]]*user_/d' \
        -e '/^listeners=/d' \
        -e '/^advertised\.listeners=/d' \
        -e '/^listener\.security\.protocol\.map=/d' \
        "$config_file"

    # For options 1 and 3: plain PLAINTEXT + CONTROLLER listeners only
    if [ "$option" = "1" ] || [ "$option" = "3" ]; then
        cat >> "$config_file" <<EOF
listeners=PLAINTEXT://:${BROKER_PORT},CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:${BROKER_PORT},CONTROLLER://localhost:9093
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
EOF
    fi

    if [ "$option" = "2a" ] || [ "$option" = "2b" ]; then
        local strip_header="true"
        [ "$option" = "2b" ] && strip_header="false"

        cat >> "$config_file" <<MLAEOF

record.fetch.plugin.classes=org.apache.kafka.server.record.mla.MLAPlugin
mla.strip.authorization.header=${strip_header}
mla.consumer.id.registry.topic=$REGISTRY_TOPIC
listeners=PLAINTEXT://:${BROKER_PORT},CONTROLLER://:9093,SASL_PLAINTEXT://:${SASL_PORT}
advertised.listeners=PLAINTEXT://localhost:${BROKER_PORT},CONTROLLER://localhost:9093,SASL_PLAINTEXT://localhost:${SASL_PORT}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
MLAEOF

        # Build JAAS config inline
        local jaas="listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required"
        jaas="$jaas username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\""
        for i in "${!CONSUMER_USERS[@]}"; do
            jaas="$jaas user_${CONSUMER_USERS[$i]}=\"${CONSUMER_PASSES[$i]}\""
        done
        jaas="$jaas;"
        echo "$jaas" >> "$config_file"
    fi

    "$KAFKA_HOME/bin/kafka-storage.sh" format --config "$config_file" \
        --cluster-id "$cluster_id" --standalone < /dev/null 2>/dev/null

    log "Starting broker (option $option)..."
    export KAFKA_HEAP_OPTS="-Xmx${BROKER_HEAP} -Xms${BROKER_HEAP}"
    "$KAFKA_HOME/bin/kafka-server-start.sh" "$config_file" \
        > "$RESULTS_DIR/broker-option${option}.log" 2>&1 < /dev/null &
    BROKER_PID=$!

    local retries=60
    while [ $retries -gt 0 ]; do
        "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1 && break
        sleep 1; retries=$((retries - 1))
    done
    [ $retries -gt 0 ] && ok "Broker started (PID $BROKER_PID)." || { err "Broker failed to start."; return 1; }
}

stop_broker() {
    if [ -n "${BROKER_PID:-}" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
        log "Stopping broker..."; kill "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true; ok "Broker stopped."
    fi
}

# ---- Topics ----
create_topics() {
    local option=$1
    case $option in
        1)
            log "Creating topic: events"
            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                --create --topic events --partitions "$PARTITIONS" \
                --replication-factor "$REPLICATION_FACTOR" 2>/dev/null
            ;;
        2a|2b)
            log "Creating registry + events topic (MLA)"
            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                --create --topic "$REGISTRY_TOPIC" --partitions 1 \
                --replication-factor "$REPLICATION_FACTOR" \
                --config cleanup.policy=compact 2>/dev/null || true

            # Build principal list for SetupRegistry
            local principals=()
            for u in "${CONSUMER_USERS[@]}"; do principals+=("User:$u"); done

            java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.SetupRegistry \
                "$BOOTSTRAP" "$REGISTRY_TOPIC" "${principals[@]}"

            log "Waiting for MLAPlugin registry client to sync..."
            sleep 10

            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                --create --topic events --partitions "$PARTITIONS" \
                --replication-factor "$REPLICATION_FACTOR" \
                --config record.fetch.plugins=org.apache.kafka.server.record.mla.MLAPlugin 2>/dev/null
            ;;
        3)
            log "Creating ${NUM_CONSUMERS} per-consumer topics"
            for i in $(seq 1 $NUM_CONSUMERS); do
                "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
                    --create --topic "event-${i}" --partitions "$PARTITIONS" \
                    --replication-factor "$REPLICATION_FACTOR" 2>/dev/null
            done
            ;;
    esac
    ok "Topics created for option $option."
}

# ---- Build filter string for option 1 ----
# Consumer at index $1 with percentage $2 filters event-ids 1..ceil(pct/100*numEventIds)
build_filter_ids() {
    local pct=$1
    local max_eid=$(echo "$pct $NUM_EVENT_IDS" | awk '{printf "%d", ($1/100)*$2 + (($1/100)*$2 > int(($1/100)*$2) ? 1 : 0)}')
    [ "$max_eid" -lt 1 ] && max_eid=1
    local ids=""
    for eid in $(seq 1 $max_eid); do
        [ -n "$ids" ] && ids="$ids,"
        ids="$ids$eid"
    done
    echo "$ids"
}

# ---- Run option ----
run_option() {
    local option=$1
    local option_dir="$RESULTS_DIR/option${option}"
    rm -rf "$option_dir"; mkdir -p "$option_dir"

    echo ""
    log "============================================"
    log "  RUNNING OPTION $option  ($NUM_CONSUMERS consumers)"
    log "============================================"
    echo ""

    start_broker "$option"
    create_topics "$option"

    # Broker resource collector
    java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
        wait "$BROKER_PID" "broker" "$option_dir/resource-broker.txt" &
    BROKER_RESOURCE_PID=$!

    # Start consumers
    local consumer_pids=()
    for i in "${!PCT_ARRAY[@]}"; do
        local pct=$(echo "${PCT_ARRAY[$i]}" | tr -d ' ')
        local cnum=$((i + 1))
        local cname="${CONSUMER_NAMES[$i]}"

        case $option in
            1)
                local filter_ids=$(build_filter_ids "$pct")
                java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                    org.apache.kafka.perf.mla.BenchmarkConsumer \
                    1 "$cnum" "$BOOTSTRAP" "events" "$filter_ids" 30 "$REPORT_INTERVAL_MS" \
                    > "$option_dir/${cname}.log" 2>&1 &
                consumer_pids+=($!)
                ;;
            2a|2b)
                java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                    org.apache.kafka.perf.mla.BenchmarkConsumer \
                    2 "$cnum" "$SASL_BOOTSTRAP" "events" "none" 60 "$REPORT_INTERVAL_MS" \
                    "${CONSUMER_USERS[$i]}" "${CONSUMER_PASSES[$i]}" \
                    > "$option_dir/${cname}.log" 2>&1 &
                consumer_pids+=($!)
                ;;
            3)
                java -Xmx${CONSUMER_HEAP} -cp "$PERF_CLASSPATH" \
                    org.apache.kafka.perf.mla.BenchmarkConsumer \
                    3 "$cnum" "$BOOTSTRAP" "event-${cnum}" "none" 30 "$REPORT_INTERVAL_MS" \
                    > "$option_dir/${cname}.log" 2>&1 &
                consumer_pids+=($!)
                ;;
        esac
    done

    # Consumer resource collectors
    sleep 2
    local consumer_resource_pids=()
    for i in "${!consumer_pids[@]}"; do
        local cpid=${consumer_pids[$i]}
        local cname="${CONSUMER_NAMES[$i]}"
        if kill -0 "$cpid" 2>/dev/null; then
            java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
                wait "$cpid" "$cname" "$option_dir/resource-${cname}.txt" &
            consumer_resource_pids+=($!)
        fi
    done

    # Producer
    log "Starting producer..."
    local producer_option="$option"
    case "$option" in 2a|2b) producer_option=2 ;; esac
    java -Xmx${PRODUCER_HEAP} -cp "$PERF_CLASSPATH" \
        org.apache.kafka.perf.mla.BenchmarkProducer \
        "$producer_option" "$BOOTSTRAP" "$NUM_EVENTS" "$EVENT_SIZE_MIN" "$EVENT_SIZE_MAX" \
        "$NUM_EVENT_IDS" "$PRODUCER_BATCH_SIZE" "$PRODUCER_LINGER_MS" "$REPORT_INTERVAL_MS" \
        "$CONSUMER_PCTS" \
        > "$option_dir/producer.log" 2>&1 &
    local PRODUCER_PID=$!

    sleep 1
    local PRODUCER_RESOURCE_PID=""
    if kill -0 "$PRODUCER_PID" 2>/dev/null; then
        java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
            wait "$PRODUCER_PID" "producer" "$option_dir/resource-producer.txt" &
        PRODUCER_RESOURCE_PID=$!
    fi

    wait "$PRODUCER_PID" 2>/dev/null || true
    ok "Producer finished."
    [ -n "$PRODUCER_RESOURCE_PID" ] && wait "$PRODUCER_RESOURCE_PID" 2>/dev/null || true

    log "Waiting for consumers..."
    for cpid in "${consumer_pids[@]}"; do wait "$cpid" 2>/dev/null || true; done
    ok "All consumers finished."
    for rpid in "${consumer_resource_pids[@]}"; do wait "$rpid" 2>/dev/null || true; done

    java -cp "$PERF_CLASSPATH" org.apache.kafka.perf.mla.ResourceCollector \
        disk "$DATA_DIR" "$option_dir/resource-disk.txt"

    stop_broker
    wait "$BROKER_RESOURCE_PID" 2>/dev/null || true
    ok "Option $option complete."
}

# ---- Report ----
generate_report() {
    local options_to_report="${1:-1 2a 2b 3}"
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local report="$RESULTS_DIR/comparison-report.txt"
    local report_ts="$RESULTS_DIR/comparison-report-${timestamp}.txt"

    cat > "$report" <<EOF
============================================================
  MLA PERFORMANCE BENCHMARK — COMPARISON REPORT
  $(date)
  Events: $(fmt_num $NUM_EVENTS), Size: ${EVENT_SIZE_MIN}-${EVENT_SIZE_MAX} bytes
  Consumers: ${NUM_CONSUMERS} (${CONSUMER_PCTS}% of events)
============================================================

EOF

    for opt in $options_to_report; do
        local dir="$RESULTS_DIR/option${opt}"
        [ ! -d "$dir" ] && continue

        echo "------------------------------------------------------------" >> "$report"
        case $opt in
            1)  echo "  OPTION 1:  Client-Side Filtering" >> "$report" ;;
            2a) echo "  OPTION 2a: MLA Broker-Side (strip ENABLED)" >> "$report" ;;
            2b) echo "  OPTION 2b: MLA Broker-Side (strip DISABLED)" >> "$report" ;;
            3)  echo "  OPTION 3:  Topic-Per-Audience Routing" >> "$report" ;;
        esac
        echo "------------------------------------------------------------" >> "$report"
        echo "" >> "$report"

        # Producer
        if [ -f "$dir/producer.log" ]; then
            echo "  PRODUCER:" >> "$report"
            grep -E "^(total_records|total_bytes|elapsed_sec|records_per_sec|mb_per_sec|avg_latency|max_latency|p50|p95|p99|errors)" \
                "$dir/producer.log" | sed 's/^/    /' >> "$report"
            echo "" >> "$report"
        fi

        # Consumers
        for i in "${!CONSUMER_NAMES[@]}"; do
            local cname="${CONSUMER_NAMES[$i]}"
            local pct=$(echo "${PCT_ARRAY[$i]}" | tr -d ' ')
            if [ -f "$dir/${cname}.log" ]; then
                echo "  ${cname} (${pct}% of events):" >> "$report"
                grep -E "^(total_records|total_bytes|elapsed_sec|accepted_records_per_sec|mb_per_sec)" \
                    "$dir/${cname}.log" | sed 's/^/    /' >> "$report"
                echo "" >> "$report"
            fi
        done

        # Resources
        echo "  RESOURCE USAGE:" >> "$report"
        echo "" >> "$report"

        local total_cpu=0 total_rss=0 total_ior=0 total_iow=0
        printf "    %-16s %10s %10s %10s %10s\n" "Component" "CPU (s)" "RSS (MB)" "IO_R (MB)" "IO_W (MB)" >> "$report"
        printf "    %-16s %10s %10s %10s %10s\n" "--------------" "--------" "--------" "---------" "---------" >> "$report"

        # Producer + broker
        for component in producer broker; do
            if [ -f "$dir/resource-${component}.txt" ]; then
                local cpu=$(grep "cpu_seconds=" "$dir/resource-${component}.txt" | cut -d= -f2)
                local rss=$(grep "peak_rss_mb=" "$dir/resource-${component}.txt" | cut -d= -f2)
                local ior=$(grep "io_read_mb=" "$dir/resource-${component}.txt" | cut -d= -f2)
                local iow=$(grep "io_write_mb=" "$dir/resource-${component}.txt" | cut -d= -f2)
                printf "    %-16s %10s %10s %10s %10s\n" "$component" "$cpu" "$rss" "$ior" "$iow" >> "$report"
                total_cpu=$(echo "$total_cpu $cpu" | awk '{printf "%.2f", $1+$2}')
                total_rss=$(echo "$total_rss $rss" | awk '{printf "%.1f", $1+$2}')
                total_ior=$(echo "$total_ior $ior" | awk '{printf "%.1f", $1+$2}')
                total_iow=$(echo "$total_iow $iow" | awk '{printf "%.1f", $1+$2}')
            fi
        done

        # Consumers
        for cname in "${CONSUMER_NAMES[@]}"; do
            if [ -f "$dir/resource-${cname}.txt" ]; then
                local cpu=$(grep "cpu_seconds=" "$dir/resource-${cname}.txt" | cut -d= -f2)
                local rss=$(grep "peak_rss_mb=" "$dir/resource-${cname}.txt" | cut -d= -f2)
                local ior=$(grep "io_read_mb=" "$dir/resource-${cname}.txt" | cut -d= -f2)
                local iow=$(grep "io_write_mb=" "$dir/resource-${cname}.txt" | cut -d= -f2)
                printf "    %-16s %10s %10s %10s %10s\n" "$cname" "$cpu" "$rss" "$ior" "$iow" >> "$report"
                total_cpu=$(echo "$total_cpu $cpu" | awk '{printf "%.2f", $1+$2}')
                total_rss=$(echo "$total_rss $rss" | awk '{printf "%.1f", $1+$2}')
                total_ior=$(echo "$total_ior $ior" | awk '{printf "%.1f", $1+$2}')
                total_iow=$(echo "$total_iow $iow" | awk '{printf "%.1f", $1+$2}')
            fi
        done

        printf "    %-16s %10s %10s %10s %10s\n" "--------------" "--------" "--------" "---------" "---------" >> "$report"
        printf "    %-16s %10s %10s %10s %10s\n" "TOTAL" "$total_cpu" "$total_rss" "$total_ior" "$total_iow" >> "$report"
        echo "" >> "$report"

        if [ -f "$dir/resource-disk.txt" ]; then
            local disk_mb=$(grep "total_mb=" "$dir/resource-disk.txt" | cut -d= -f2)
            echo "  DISK USAGE: ${disk_mb} MB" >> "$report"
        fi
        echo "" >> "$report"
    done

    echo "============================================================" >> "$report"
    echo "  END OF REPORT" >> "$report"
    echo "============================================================" >> "$report"

    ok "Comparison report: $report"
    cp "$report" "$report_ts"
    ok "Timestamped copy:  $report_ts"
    echo ""
    cat "$report"
}

# ---- Main ----
main() {
    local run_options=""

    while [ $# -gt 0 ]; do
        case "$1" in
            --nr-events) NUM_EVENTS="$2"; shift 2 ;;
            0|1|2a|2b|3|all) run_options="$1"; shift ;;
            *) echo "Usage: $0 [--nr-events N] [0|1|2a|2b|3]"; exit 1 ;;
        esac
    done

    if [ -z "$run_options" ]; then
        echo ""
        echo "  MLA Performance Benchmark ($NUM_CONSUMERS consumers)"
        echo "  ========================="
        echo "  0  = Run all options"
        echo "  1  = Client-side filtering"
        echo "  2a = MLA broker-side (strip ENABLED)"
        echo "  2b = MLA broker-side (strip DISABLED)"
        echo "  3  = Topic-per-audience routing"
        echo ""
        read -p "  Select option [0]: " run_options
        run_options="${run_options:-0}"
    fi
    [ "$run_options" = "0" ] && run_options="all"

    log "MLA Performance Benchmark"
    log "Events: $(fmt_num $NUM_EVENTS), Size: ${EVENT_SIZE_MIN}-${EVENT_SIZE_MAX} bytes"
    log "Consumers: $NUM_CONSUMERS (${CONSUMER_PCTS}%)"
    log "Broker heap: $BROKER_HEAP, Producer heap: $PRODUCER_HEAP, Consumer heap: $CONSUMER_HEAP"
    echo ""

    build_benchmark
    mkdir -p "$RESULTS_DIR"
    trap 'stop_broker' EXIT

    if [ "$run_options" = "all" ]; then
        run_option 1; run_option 2a; run_option 2b; run_option 3
        generate_report "1 2a 2b 3"
    else
        run_option "$run_options"
        generate_report "$run_options"
    fi
}

main "$@"
