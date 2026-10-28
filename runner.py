import asyncio
import json
import logging
import random
import sqlite3
import time
import uuid
from typing import Any, Callable, Dict, List, Optional
import websockets
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AdaptiveConnectionManager")

# ==========================================
# 1. Durable SQLite Outbox Store
# ==========================================
class SQLiteOutboxStore:
    def __init__(self, db_path: str = "mobile_outbox.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initializes the SQLite outbox table with proper indexes."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox_intents (
                    id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    retries INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.commit()

    def add(self, record: Dict[str, Any]):
        """Persists a new intent to disk."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO outbox_intents (id, payload, timestamp, retries)
                VALUES (?, ?, ?, ?)
                """,
                (
                    record["id"],
                    json.dumps(record["payload"]),
                    record["timestamp"],
                    record["retries"],
                ),
            )
            conn.commit()

    def remove(self, intent_id: str):
        """Removes a successfully acknowledged intent from storage."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM outbox_intents WHERE id = ?", (intent_id,))
            conn.commit()

    def increment_retry(self, intent_id: str):
        """Increments retry counter for a failed transmission attempt."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE outbox_intents SET retries = retries + 1 WHERE id = ?",
                (intent_id,),
            )
            conn.commit()

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all pending unacknowledged intents ordered by timestamp."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT id, payload, timestamp, retries FROM outbox_intents ORDER BY timestamp ASC")
            rows = cursor.fetchall()
            
            return [
                {
                    "id": row["id"],
                    "payload": json.loads(row["payload"]),
                    "timestamp": row["timestamp"],
                    "retries": row["retries"],
                }
                for row in rows
            ]

    def count(self) -> int:
        """Returns the current count of pending intents."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM outbox_intents")
            return cursor.fetchone()[0]


# ==========================================
# 2. Persistent Adaptive Connection Manager
# ==========================================
class PersistentAdaptiveConnectionManager:
    def __init__(
        self,
        uri: str,
        db_path: str = "mobile_outbox.db",
        initial_timeout_guess: float = 45.0,
        safety_margin_min: float = 0.70,
        safety_margin_max: float = 0.85,
        max_backoff: float = 60.0,
    ):
        self.uri = uri
        self.initial_timeout_guess = initial_timeout_guess
        self.safety_margin_min = safety_margin_min
        self.safety_margin_max = safety_margin_max
        self.max_backoff = max_backoff

        # Core state variables & metrics
        self.learned_timeout: float = initial_timeout_guess
        self.consecutive_failures: int = 0
        self.last_heartbeat_rtt: float = 0.0
        self.is_protected: bool = False
        self._is_running: bool = False
        self.current_backoff: float = 1.0

        # Connection & Tasks
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._session_start_time: float = 0.0

        # Durable Storage Backing
        self.store = SQLiteOutboxStore(db_path)

        # Event callbacks registry
        self._callbacks: Dict[str, List[Callable]] = {
            "connected": [],
            "disconnected": [],
            "message_received": [],
            "intent_queued": [],
            "intent_flushed": [],
        }

    def on(self, event: str, callback: Callable):
        """Register an event listener/callback."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    async def _emit(self, event: str, data: Any = None):
        """Trigger registered callbacks safely."""
        for cb in self._callbacks.get(event, []):
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(data)
                else:
                    cb(data)
            except Exception as e:
                logger.error(f"Error in callback for {event}: {e}")

    def protect(self):
        """Simulate acquiring a foreground service / wake lock protection."""
        self.is_protected = True
        logger.info("[Manager] State changed: PROTECTED (Foreground mode active)")

    def unprotect(self):
        """Simulate releasing foreground protection."""
        self.is_protected = False
        logger.info("[Manager] State changed: UNPROTECTED (Background mode)")

    async def start(self):
        """Starts the connection manager main loop."""
        self._is_running = True
        logger.info("Starting PersistentAdaptiveConnectionManager...")
        while self._is_running:
            try:
                logger.info(f"Connecting to {self.uri}...")
                async with websockets.connect(self.uri, ping_interval=None) as ws:
                    self.websocket = ws
                    self._session_start_time = time.time()
                    self.consecutive_failures = 0
                    self.current_backoff = 1.0

                    logger.info("Connected successfully. Resetting backoff.")
                    await self._emit("connected", {"uri": self.uri, "timestamp": time.time()})
                    
                    # Flush pending intents from SQLite outbox upon reconnection
                    await self.flush_outbox()

                    # Start concurrent operational loops
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    self._receive_task = asyncio.create_task(self._receive_loop())

                    done, pending = await asyncio.wait(
                        [self._heartbeat_task, self._receive_task],
                        return_when=asyncio.FIRST_EXCEPTION,
                    )

                    for task in pending:
                        task.cancel()
                    for task in done:
                        if task.exception():
                            raise task.exception()

            except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                self._handle_unexpected_disconnect(e)
            except Exception as e:
                logger.error(f"Unexpected error in connection loop: {e}", exc_info=True)
                self._handle_unexpected_disconnect(e)

            if self._is_running:
                await self._handle_backoff()

    def _handle_unexpected_disconnect(self, error: Exception):
        """Adapts timeout metrics and updates failure counters upon drop."""
        self.consecutive_failures += 1
        session_duration = time.time() - self._session_start_time if self._session_start_time else 0
        
        logger.warning(f"Connection lost: {error} (Session duration: {session_duration:.2f}s)")

        # Timeout Learning (EMA): If dropped early, shrink timeout guess
        if session_duration < self.learned_timeout:
            self.learned_timeout = max(15.0, (0.8 * self.learned_timeout) + (0.2 * session_duration))
            logger.info(f"Adaptive Timeout shrunk. New learned timeout: {self.learned_timeout:.2f}s")

        asyncio.create_task(self._emit("disconnected", {"error": str(error), "failures": self.consecutive_failures}))

    async def _heartbeat_loop(self):
        """Schedules heartbeats with safety margin (70-85% of learned timeout) + jitter."""
        try:
            while self._is_running and self.websocket:
                margin = random.uniform(self.safety_margin_min, self.safety_margin_max)
                interval = self.learned_timeout * margin
                
                await asyncio.sleep(interval)

                if self.websocket and self.websocket.open:
                    ping_start = time.time()
                    pong_waiter = await self.websocket.ping()
                    await asyncio.wait_for(pong_waiter, timeout=5.0)
                    
                    self.last_heartbeat_rtt = time.time() - ping_start
                    logger.debug(f"Heartbeat OK. RTT: {self.last_heartbeat_rtt * 1000:.2f}ms")

                    # Reward learned timeout upward toward stability max
                    if self.learned_timeout < 120.0:
                        self.learned_timeout = min(120.0, self.learned_timeout * 1.02)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"Heartbeat failed: {e}. Forcing socket close.")
            if self.websocket:
                await self.websocket.close()

    async def _receive_loop(self):
        """Continuously reads inbound messages."""
        try:
            async for message in self.websocket:
                try:
                    payload = json.loads(message)
                    await self._emit("message_received", payload)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse incoming message: {message}")
        except asyncio.CancelledError:
            pass

    async def enqueue_intent(self, intent_payload: dict) -> str:
        """Persists intent to SQLite outbox first, then attempts immediate transmission."""
        intent_id = str(uuid.uuid4())
        record = {
            "id": intent_id,
            "payload": intent_payload,
            "timestamp": time.time(),
            "retries": 0
        }

        # 1. Write-Ahead Guarantee to SQLite
        self.store.add(record)
        logger.info(f"Intent {intent_id} committed to SQLite outbox.")
        await self._emit("intent_queued", record)

        # 2. Try sending immediately if connected
        if self.websocket and self.websocket.open:
            success = await self._send_raw(record)
            if success:
                self.store.remove(intent_id)
                logger.info(f"Intent {intent_id} transmitted and removed from outbox.")
                
        return intent_id

    async def _send_raw(self, record: dict) -> bool:
        try:
            await self.websocket.send(json.dumps(record))
            return True
        except Exception as e:
            logger.error(f"Failed to transmit record {record['id']}: {e}")
            return False

    async def flush_outbox(self):
        """Flushes all persisted SQLite intents upon reconnection."""
        pending_records = self.store.get_all()
        if not pending_records:
            return

        logger.info(f"Flushing SQLite outbox: {len(pending_records)} pending intents.")
        for record in pending_records:
            success = await self._send_raw(record)
            if success:
                self.store.remove(record["id"])
                await self._emit("intent_flushed", record)
            else:
                self.store.increment_retry(record["id"])

    async def _handle_backoff(self):
        """Quality-aware exponential backoff with full jitter."""
        multiplier = 0.5 if self.is_protected else 1.0
        jitter = random.uniform(0, 1.0)
        sleep_time = min(self.max_backoff, (self.current_backoff * multiplier)) + jitter
        
        logger.info(f"Backing off for {sleep_time:.2f}s (Failures: {self.consecutive_failures}, Protected: {self.is_protected})")
        await asyncio.sleep(sleep_time)
        
        self.current_backoff = min(self.max_backoff, self.current_backoff * 2)

    def get_metrics(self) -> dict:
        """Returns current connection health and adaptive metrics."""
        return {
            "learned_timeout_seconds": round(self.learned_timeout, 2),
            "consecutive_failures": self.consecutive_failures,
            "last_heartbeat_rtt_ms": round(self.last_heartbeat_rtt * 1000, 2),
            "is_protected": self.is_protected,
            "outbox_queue_size": self.store.count(),
            "is_connected": bool(self.websocket and self.websocket.open)
        }

    async def stop(self):
        """Gracefully shuts down the manager."""
        self._is_running = False
        if self.websocket:
            await self.websocket.close()
        logger.info("PersistentAdaptiveConnectionManager stopped.")


# ==========================================
# 3. Example Runnable Simulation
# ==========================================
async def main():
    manager = PersistentAdaptiveConnectionManager(
        uri="wss://echo.websocket.events",
        db_path="test_mobile_outbox.db"
    )

    # Register event listeners
    manager.on("connected", lambda data: print(f"-> EVENT: Connected to {data['uri']}"))
    manager.on("disconnected", lambda data: print(f"-> EVENT: Disconnected (Failures: {data['failures']})"))
    manager.on("intent_queued", lambda rec: print(f"-> EVENT: Intent queued -> {rec['id']}"))
    manager.on("intent_flushed", lambda rec: print(f"-> EVENT: Intent flushed -> {rec['id']}"))

    # Simulate foreground protection mode
    manager.protect()

    # Start manager in background task
    task = asyncio.create_task(manager.start())

    # Wait a moment, then enqueue a transaction intent
    await asyncio.sleep(3)
    await manager.enqueue_intent({"action": "double_tap_pay", "amount": 500, "currency": "NGN"})

    # Let it run for 10 seconds
    await asyncio.sleep(10)

    # Print current metrics
    print("\n--- Current Metrics ---")
    print(json.dumps(manager.get_metrics(), indent=2))

    # Stop manager
    await manager.stop()
    await task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")