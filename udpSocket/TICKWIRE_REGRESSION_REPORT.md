# TickWire Regression Test Report

## Run Information

- **Run directory:** `logs/tickwire-checklist-20261022-154414`
- **Date:** 2026-10-22
- **Scenarios:** 7
- **Messages expected per scenario:** 10,000
- **Required final sequence:** 15000
- **Overall result:** **7/7 passed**

Each scenario used a fresh receiver. The publisher was started only after the
receiver was running, and its Enter prompt was held until the receiver
completed.

## Aggregate Results

| Metric | Result |
|---|---:|
| Scenarios passed | 7 / 7 |
| Scenarios failed | 0 |
| Total messages processed | 70,000 |
| Receivers reaching `[COMPLETE]` | 7 / 7 |
| Publisher failures | 0 |
| Log errors detected | 0 |
| Total measured receiver time | 61.313397 s |
| Average measured time | 8.759057 s |
| Fastest case | Normal — 0.136871 s |
| Slowest case | Both feeds drop — 12.553117 s |
| Average throughput across cases | 15,754.19 messages/s |

## Per-Case Results

| Case | Send interval | Processed | Completion | Time | Throughput | Result |
|---|---:|---:|---|---:|---:|---|
| Unpaced normal burst | 0 ms | 10,000 | Through sequence 15000 | 0.136871 s | 73,061.52 msg/s | **PASS** |
| Missing tail | 0 ms | 10,000 | Through sequence 15000 | 0.302024 s | 33,109.92 msg/s | **PASS** |
| Feed A drop | 1 ms | 10,000 | Through sequence 15000 | 11.968916 s | 835.50 msg/s | **PASS** |
| Feed B drop | 1 ms | 10,000 | Through sequence 15000 | 12.098369 s | 826.56 msg/s | **PASS** |
| Both feeds drop | 1 ms | 10,000 | Through sequence 15000 | 12.553117 s | 796.61 msg/s | **PASS** |
| Duplicate packet | 1 ms | 10,000 | Through sequence 15000 | 12.175950 s | 821.29 msg/s | **PASS** |
| Reordered packet | 1 ms | 10,000 | Through sequence 15000 | 12.078150 s | 827.94 msg/s | **PASS** |

## Detailed Evaluation

### 1. Unpaced Normal Burst

- **Command:** `TICKWIRE_VERBOSE=0 TICKWIRE_SEND_INTERVAL_MS=0 ./publisher normal`
- Processed all 10,000 messages.
- Reached:
  `[COMPLETE] Processed all 10000 messages through sequence 15000`
- No TCP recovery was required.
- **Evaluation:** **PASS**

### 2. Missing Tail

- **Command:** `TICKWIRE_VERBOSE=0 TICKWIRE_SEND_INTERVAL_MS=0 ./publisher drop-tail`
- Processed all 10,000 messages.
- Bulk recovery requested sequences `14750-15000`.
- Bulk recovery returned exactly **251 messages**.
- Reached completion through sequence 15000.
- **Evaluation:** **PASS**

### 3. Feed A Drop

- **Command:** `TICKWIRE_SEND_INTERVAL_MS=1 ./publisher drop-a`
- Publisher dropped sequence **5005 on Feed A only**.
- Processed all 10,000 messages.
- Feed B redundancy supplied the missing message.
- No TCP recovery was required.
- **Evaluation:** **PASS**

### 4. Feed B Drop

- **Command:** `TICKWIRE_SEND_INTERVAL_MS=1 ./publisher drop-b`
- Publisher dropped sequence **5005 on Feed B only**.
- Processed all 10,000 messages.
- Feed A redundancy supplied the missing message.
- No TCP recovery was required.
- **Evaluation:** **PASS**

### 5. Both Feeds Drop Selected Packets

- **Command:** `TICKWIRE_SEND_INTERVAL_MS=1 ./publisher drop-both`
- Both UDP feeds skipped these sequences:

  ```text
  5005, 6005, 7005, 8005, 9005,
  10005, 11005, 12005, 13005, 14005
  ```

- TCP successfully retransmitted all 10 missing sequences.
- Processed all 10,000 messages.
- Reached completion through sequence 15000.

#### TCP Recovery Statistics

| Metric | Result |
|---|---:|
| Successful recoveries | 10 |
| Mean recovery latency | 1547 us |
| p50 latency | 1176 us |
| p95 latency | 2948 us |
| p99 latency | 2948 us |

- **Evaluation:** **PASS**

### 6. Duplicate Packet

- **Command:** `TICKWIRE_SEND_INTERVAL_MS=1 ./publisher duplicate`
- Publisher duplicated UDP sequence **5005**.
- Receiver processed exactly 10,000 messages, not 10,001.
- Completion reached through sequence 15000.
- **Evaluation:** **PASS**
- **Conclusion:** Duplicate suppression and exactly-once processing behavior passed.

### 7. Reordered Packet

- **Command:** `TICKWIRE_SEND_INTERVAL_MS=1 ./publisher reorder`
- Publisher delayed sequence **5005** and sent it after sequence **5006**.
- Receiver processed all 10,000 messages in sequence order.
- Completion reached through sequence 15000.
- **Evaluation:** **PASS**
- **Conclusion:** Ordered delivery was preserved despite packet reordering.

## Overall Analysis

The regression run validates the core TickWire delivery guarantees:

1. **Completeness:** Every scenario processed all 10,000 expected messages.
2. **Stream termination:** Every receiver completed at sequence 15000.
3. **Tail recovery:** The missing-tail case recovered the exact requested range
   of 251 messages.
4. **Feed redundancy:** A loss on either individual UDP feed was handled by
   the other feed without TCP recovery.
5. **TCP recovery:** Simultaneous losses on both feeds were recovered
   successfully, with a mean recovery latency of 1547 us.
6. **Duplicate protection:** Injected duplicate packets did not increase the
   processed-message count.
7. **Ordering:** Delayed and reordered packets were ultimately processed in
   sequence order.
8. **Operational reliability:** All publishers exited successfully, all
   receivers reached completion, and no run logs contained detected errors.

The lower throughput in fault-injection cases is expected because those tests
use a 1 ms send interval and exercise recovery/order-management paths. The
unpaced normal and missing-tail cases use a 0 ms interval and therefore show
substantially higher throughput.

## Artifacts

Detailed receiver and publisher logs are stored under:

`logs/tickwire-checklist-20261022-154414/`

The report is based on the following captured files:

- `normal-receiver.log` and `normal-publisher.log`
- `drop-tail-receiver.log` and `drop-tail-publisher.log`
- `drop-a-receiver.log` and `drop-a-publisher.log`
- `drop-b-receiver.log` and `drop-b-publisher.log`
- `drop-both-receiver.log` and `drop-both-publisher.log`
- `duplicate-receiver.log` and `duplicate-publisher.log`
- `reorder-receiver.log` and `reorder-publisher.log`
