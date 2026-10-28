# Persistent Adaptive WebSocket Connection Manager

## Overview

This project demonstrates a resilient asynchronous WebSocket client designed for mobile or IoT-style environments where network connections may be unreliable. The implementation is contained in `runner.py` and centers on two classes:

- `SQLiteOutboxStore` — provides durable local storage for outbound messages.
- `PersistentAdaptiveConnectionManager` — manages the WebSocket connection, heartbeats, reconnection, callbacks, message delivery, and connection metrics.

The script also contains a runnable simulation that connects to a public echo WebSocket server, queues a sample payment intent, prints connection metrics, and shuts down.

## Main Objectives

The implementation attempts to provide:

1. Automatic WebSocket connection and reconnection.
2. Adaptive heartbeat scheduling.
3. Exponential reconnect backoff with random jitter.
4. Durable storage of outbound intents in SQLite.
5. Automatic retry of queued intents after reconnection.
6. Event callbacks for important connection and message events.
7. Basic runtime connection-health metrics.
8. A simulated foreground or protected operating mode.

## Architecture

### `SQLiteOutboxStore`

`SQLiteOutboxStore` is a synchronous SQLite storage layer used to preserve outbound intents across application restarts.

By default, it writes to `mobile_outbox.db`, although the database path can be supplied when the store or connection manager is created.

During initialization, it creates an `outbox_intents` table if one does not already exist. Each row contains:

| Column | Purpose |
| --- | --- |
| `id` | Unique UUID identifying the intent |
| `payload` | JSON-encoded intent payload |
| `timestamp` | Time at which the intent was created |
| `retries` | Number of failed retransmission attempts |

The store supports the following operations:

- `add(record)` inserts or replaces an intent.
- `remove(intent_id)` deletes an intent.
- `increment_retry(intent_id)` increases its retry counter.
- `get_all()` retrieves pending intents in creation order.
- `count()` returns the number of pending intents.

This provides a write-ahead outbox: an intent is saved before the application attempts to send it, so it is not immediately lost if the process exits or the network becomes unavailable.

### `PersistentAdaptiveConnectionManager`

This class controls the complete connection lifecycle.

Its configurable values include:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `uri` | Required | WebSocket endpoint |
| `db_path` | `mobile_outbox.db` | SQLite outbox location |
| `initial_timeout_guess` | 45 seconds | Initial estimated idle timeout |
| `safety_margin_min` | 0.70 | Minimum heartbeat timing multiplier |
| `safety_margin_max` | 0.85 | Maximum heartbeat timing multiplier |
| `max_backoff` | 60 seconds | Maximum reconnect delay |

It tracks the following runtime state:

- Current WebSocket connection.
- Whether the manager is running.
- Learned timeout estimate.
- Last heartbeat round-trip time.
- Consecutive failure count.
- Current reconnect backoff.
- Protected or foreground status.
- Heartbeat and receive-loop tasks.
- Current connection session start time.

## Connection Lifecycle

Calling `start()` begins a loop that continues until `stop()` is called.

For each connection attempt, the manager:

1. Connects to the configured WebSocket URI.
2. Disables the `websockets` package's automatic ping interval.
3. Stores the new connection and session start time.
4. Resets the failure count and reconnect delay.
5. Emits the `connected` event.
6. Flushes pending records from SQLite.
7. Starts the heartbeat and receive loops concurrently.
8. Waits for an operational task to fail.
9. Handles the disconnection.
10. Waits according to its reconnect backoff before trying again.

Expected connection, operating-system, and timeout errors are handled as disconnections. Unexpected exceptions are logged with a traceback and then handled through the same reconnect path.

## Adaptive Heartbeats

The manager implements its own WebSocket ping schedule instead of using the library's default heartbeat behavior.

For each heartbeat, it selects a random safety margin between 70% and 85% and multiplies it by the currently learned timeout:

```text
heartbeat interval = learned timeout × random safety margin
```

With the initial 45-second timeout estimate, the first heartbeat normally occurs between 31.5 and 38.25 seconds after the previous iteration begins.

When a ping succeeds:

- The round-trip time is recorded.
- The learned timeout increases by 2%.
- The learned timeout is capped at 120 seconds.

If a connection ends sooner than the learned timeout, the manager reduces its estimate using an exponential moving average:

```text
new timeout = 80% of old timeout + 20% of session duration
```

The learned timeout cannot fall below 15 seconds.

The purpose is to send heartbeats before a router, proxy, mobile network, or load balancer closes an apparently idle connection, while avoiding unnecessarily frequent network activity.

## Reconnection Backoff

After a disconnection, the manager waits before reconnecting. The backoff begins at one second and doubles after each failed cycle until it reaches the configured maximum of 60 seconds.

A random value between zero and one second is added as jitter. Jitter helps prevent many clients from reconnecting at exactly the same time.

The manager also supports a simulated protected state:

- `protect()` marks the manager as being in foreground or protected mode.
- `unprotect()` marks it as being in background mode.

Protected mode halves the base reconnect delay. These methods only alter the manager's internal behavior; they do not acquire a real Android foreground service, operating-system wake lock, or equivalent platform resource.

## Durable Intent Delivery

`enqueue_intent()` creates a record containing:

- A generated UUID.
- The supplied payload.
- A creation timestamp.
- A retry count starting at zero.

The method saves the record to SQLite before attempting network transmission. It also emits an `intent_queued` event.

If a WebSocket connection is available, the manager immediately tries to send the complete record as JSON. If transmission succeeds, it removes the record from SQLite. Otherwise, the record remains available for a later retry.

Whenever a new connection is established, `flush_outbox()` loads all pending records in chronological order and tries to send each one:

- Successfully sent records are deleted and produce an `intent_flushed` event.
- Failed records remain in SQLite and have their retry counter incremented.

## Receiving Messages

The receive loop continuously reads messages from the WebSocket connection.

Each message is parsed as JSON. Successfully parsed data produces a `message_received` event. Invalid JSON is logged and discarded without stopping the connection manager.

## Event System

Callbacks can be registered with `on(event, callback)`. The supported events are:

| Event | Trigger |
| --- | --- |
| `connected` | A WebSocket connection is established |
| `disconnected` | A connection failure is handled |
| `message_received` | A valid JSON message is received |
| `intent_queued` | An intent is written to SQLite |
| `intent_flushed` | A queued intent is successfully retransmitted |

Both synchronous and asynchronous callbacks are supported. Callback exceptions are caught and logged so that one failing callback does not directly stop the manager.

## Metrics

`get_metrics()` returns a dictionary containing:

- `learned_timeout_seconds` — current adaptive timeout estimate.
- `consecutive_failures` — current failure counter.
- `last_heartbeat_rtt_ms` — most recent heartbeat latency.
- `is_protected` — whether protected mode is enabled.
- `outbox_queue_size` — number of records currently stored in SQLite.
- `is_connected` — whether the WebSocket is considered open.

## Runnable Simulation

The `main()` function demonstrates the manager using:

```text
wss://echo.websocket.events
```

It uses `test_mobile_outbox.db` for storage and performs the following sequence:

1. Creates the connection manager.
2. Registers callbacks that print connection and outbox events.
3. Enables protected mode.
4. Starts the manager in a background task.
5. Waits three seconds.
6. Queues a sample `double_tap_pay` intent for 500 NGN.
7. Runs for another ten seconds.
8. Prints current metrics.
9. Stops the manager and waits for shutdown.

The sample intent is:

```json
{
  "action": "double_tap_pay",
  "amount": 500,
  "currency": "NGN"
}
```

The module runs this simulation when `runner.py` is executed directly.

## Running the Script

The implementation requires Python, SQLite support, and the third-party `websockets` package.

Install the WebSocket dependency:

```bash
pip install websockets
```

Run the example:

```bash
python runner.py
```

The public echo endpoint requires internet access and may be unavailable or changed independently of this project.
