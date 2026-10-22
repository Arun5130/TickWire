#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

LOG_DIR="logs/tickwire-regression-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOG_DIR"

RECEIVER_PID=""

cleanup() {
    if [[ -n "$RECEIVER_PID" ]] &&
       kill -0 "$RECEIVER_PID" 2>/dev/null; then
        kill -INT "$RECEIVER_PID" 2>/dev/null || true
        wait "$RECEIVER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

echo "Building TickWire..."

g++ -std=c++20 -Wall -Wextra -Wpedantic -O2 -pthread \
    receiver.cpp -o receiver

g++ -std=c++20 -Wall -Wextra -Wpedantic -O2 -pthread \
    publisher.cpp -o publisher

SCENARIOS=(
    normal
    drop-a
    drop-b
    duplicate
    reorder
    drop-both
)

for scenario in "${SCENARIOS[@]}"; do
    echo
    echo "=================================="
    echo "Testing scenario: $scenario"
    echo "=================================="

    RX_LOG="$LOG_DIR/${scenario}-receiver.log"
    PUB_LOG="$LOG_DIR/${scenario}-publisher.log"
    SEQ_FILE="$LOG_DIR/${scenario}-sequences.txt"

    ./receiver >"$RX_LOG" 2>&1 &
    RECEIVER_PID=$!

    # Allow both UDP sockets to bind.
    sleep 0.3

    SEND_INTERVAL_MS="${TICKWIRE_SEND_INTERVAL_MS:-20}"

    # Keep the TCP recovery server alive until sending should be finished,
    # plus an additional second for recovery to complete.
    HOLD_SECONDS=$(( (100 * SEND_INTERVAL_MS + 999) / 1000 + 1 ))

    if ! (sleep "$HOLD_SECONDS"; printf '\n') |
        ./publisher "$scenario" >"$PUB_LOG" 2>&1; then
        echo "FAIL: Publisher failed in $scenario"
        echo "Inspect $PUB_LOG"
        exit 1
    fi

    # Wait until all 100 messages have been processed.
    count=0
    for attempt in {1..60}; do
        count=$(grep -c '^Processed sequence:' "$RX_LOG" || true)

        if [[ "$count" -ge 100 ]]; then
            break
        fi

        sleep 0.1
    done

    kill -INT "$RECEIVER_PID" 2>/dev/null || true
    wait "$RECEIVER_PID" 2>/dev/null || true
    RECEIVER_PID=""

    # Extract only successfully processed messages.
    awk '/^Processed sequence:/ { print $3 }' \
        "$RX_LOG" >"$SEQ_FILE"

    count=$(wc -l <"$SEQ_FILE")

    if [[ "$count" -ne 100 ]]; then
        echo "FAIL: $scenario processed $count messages; expected 100."
        tail -n 40 "$RX_LOG"
        exit 1
    fi

    # Require exactly 5001, 5002, ..., 5100 in processing order.
    if ! awk '
        BEGIN { expected = 5001; bad = 0 }
        {
            if ($1 != expected) {
                printf "Expected %d, got %d\n", expected, $1
                bad = 1
            }
            expected++
        }
        END {
            if (expected != 5101) bad = 1
            exit bad
        }
    ' "$SEQ_FILE"; then
        echo "FAIL: Ordering or duplicate-processing error in $scenario."
        exit 1
    fi

    # Both feeds lose 5005 only in the drop-both scenario.
    if [[ "$scenario" == "drop-both" ]]; then
        if ! grep -q '^RECOVERED Sequence 5005 via TCP$' "$RX_LOG"; then
            echo "FAIL: TCP did not recover sequence 5005."
            exit 1
        fi
    else
        if grep -q '^\[RECOVERY QUEUED\]' "$RX_LOG"; then
            echo "FAIL: Unexpected TCP recovery in $scenario."
            grep '^\[RECOVERY QUEUED\]' "$RX_LOG"
            exit 1
        fi
    fi

    echo "PASS: $scenario"
    echo "      100 messages processed exactly once and in order."

    if [[ "$scenario" == "drop-both" ]]; then
        echo "      Sequence 5005 recovered over TCP."
    else
        echo "      No TCP recovery requests needed."
    fi
done

echo
echo "=================================="
echo "ALL SIX TICKWIRE TESTS PASSED"
echo "=================================="
echo "Logs saved in: $LOG_DIR"