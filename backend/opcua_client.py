"""
backend/opcua_client.py
=======================
OPC UA client for the Siemens S7-1512SP PLC, wrapped in a QThread so it runs
alongside the PyQt5 event loop without blocking the GUI.

Architecture overview
---------------------
PlcClient (QThread)
  └─ run()                        ← worker thread entry
       ├─ _connect()              ← builds node map, registers subscriptions
       ├─ _poll_loop()            ← 100 ms idle loop; subscriptions drive live events
       └─ _cleanup()              ← tears down subscription + disconnects

OpcUaSubscriptionHandler          ← opcua library callback, routes to PlcClient signals
  └─ datachange_notification()    ← called by opcua's internal subscription thread

"""

from __future__ import annotations

import logging
import struct
import threading
import time
from typing import Optional

from opcua import Client, ua
from PyQt5.QtCore import QThread, pyqtSignal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

PLC_URL    = "opc.tcp://172.21.3.1:4840"
SIEMENS_URI = "http://www.siemens.com/simatic-s7-opcua"

# OPC UA subscription publish interval (ms)
SUBSCRIPTION_INTERVAL_MS = 500

# Poll loop sleep between iterations (seconds)
POLL_INTERVAL_S = 0.1

# Timeout waiting for PLC to confirm RFID write (seconds)
WRITE_TIMEOUT_S = 5.0

# Reconnection backoff: starts at BASE, doubles each attempt, caps at MAX
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S  = 30.0

# ---------------------------------------------------------------------------
# OPC UA node path table
# All paths are relative to the Siemens namespace; keys are the aliases used
# throughout this module.
# ---------------------------------------------------------------------------

NODE_PATHS: dict[str, str] = {
    "conv_start": '"conv_start"',
    # abstractMachine [DB1] — GRAPH sequence control
    "taskCode":   '"abstractMachine"."taskCode"',   # Byte  — TCP byte 0 to Festo CECC
    "awaitApp":   '"abstractMachine"."awaitApp"',   # Bool  — PLC waiting for MES command
    "appRun":     '"abstractMachine"."appRun"',     # Bool  — MES sets True to start cycle
    "appDone":    '"abstractMachine"."appDone"',    # Bool  — PLC sets True when done
    "sendTCPcmd": '"abstractMachine"."sendTCPcmd"', # Bool  — triggers 2-byte TCP frame
    "drillDone":  '"abstractMachine"."drillDone"',  # Bool  — drilling op complete
    "release":    '"abstractMachine"."release"',    # Bool  — release / reset

    # rfidControl [DB3] — RF210R IO-Link reader/writer
    "doRead":       '"rfidControl"."doRead"',       # Bool  — MES triggers read
    "readDone":     '"rfidControl"."readDone"',     # Bool  — PLC confirms read complete
    "readPresence": '"rfidControl"."readPresence"', # Bool  — tag detected at reader
    "doWrite":      '"rfidControl"."doWrite"',      # Bool  — MES triggers write
    "writeDone":    '"rfidControl"."writeDone"',    # Bool  — PLC confirms write complete
    "writePresence":'"rfidControl"."writePresence"',# Bool  — tag detected for write

    # identData [DB2] — 32-byte RFID tag buffer
    "readData":  '"identData"."readData"',          # Array[0..31] of Byte
    "writeData": '"identData"."writeData"',         # Array[0..31] of Byte
}

# Nodes that the OPC UA subscription monitors for event-driven callbacks.
# The current event flow depends on these subscriptions; _poll_loop() is idle
# until periodic reads are added there.
SUBSCRIBED_ALIASES: frozenset[str] = frozenset({
    "appDone",
    "conv_start",
    "awaitApp",
    "readDone",
    "readPresence",
})

# ---------------------------------------------------------------------------
# Recipe → task code mapping (TCP byte 0 sent to Festo CECC)
# ---------------------------------------------------------------------------

RECIPE_TASK_CODES: dict[str, int] = {
    "No Holes":    0,
    "Left Holes":  1,
    "Right Holes": 2,
    "All Holes":   3,
}

# ---------------------------------------------------------------------------
# RFID data encoding / decoding
# Format (32 bytes, big-endian):
#   bytes  0-3  : order_id  (uint32)
#   bytes  4-7  : task_code (uint32)
#   bytes  8-11 : quantity  (uint32)
#   bytes 12-31 : zero-padded
# ---------------------------------------------------------------------------

RFID_DATA_SIZE = 32


def encode_rfid(order_id: int, task_code: int, quantity: int) -> list[int]:
    """
    Serialize order fields into a 32-byte list for identData.writeData.

    :param order_id:  Numeric production order identifier (fits in uint32).
    :param task_code: Recipe code 0-3 matching RECIPE_TASK_CODES.
    :param quantity:  Units to produce (fits in uint32).
    :returns:         List of 32 ints (0-255) ready to write to the PLC node.
    """
    buf = bytearray(RFID_DATA_SIZE)
    struct.pack_into(">I", buf, 0, order_id)
    struct.pack_into(">I", buf, 4, task_code)
    struct.pack_into(">I", buf, 8, quantity)
    return list(buf)


def format_rfid_trace(order_id: int, task_code: int, quantity: int) -> str:
    """
    Human-readable summary of the RFID payload written to the carrier.

    The current reduced-scope MES flow uses the production_orders.id integer as
    the short-term execution key sent through the PLC/RFID path.  The business
    order identifier remains stored separately in SQLite and shown in the UI.
    """
    return f"dbid={order_id};task={task_code};qty={quantity}"


def decode_rfid(data: list[int]) -> dict[str, int]:
    """
    Deserialize a 32-byte RFID payload from identData.readData.

    :param data:   List of ints returned by node.get_value() on the PLC array.
    :returns:      Dict with keys "order_id", "task_code", "quantity".
    :raises ValueError: If the payload is shorter than 12 bytes.
    """
    if len(data) < 12:
        raise ValueError(
            f"RFID readData too short: got {len(data)} bytes, expected >= 12"
        )
    raw = bytes(data)
    return {
        "order_id":  struct.unpack_from(">I", raw, 0)[0],
        "task_code": struct.unpack_from(">I", raw, 4)[0],
        "quantity":  struct.unpack_from(">I", raw, 8)[0],
    }


# ---------------------------------------------------------------------------
# OPC UA subscription handler
# ---------------------------------------------------------------------------

class OpcUaSubscriptionHandler:
    """
    Implements the opcua SubHandler interface.

    The opcua library calls datachange_notification() from its own internal
    subscription thread whenever a monitored node value changes.  This class
    translates those raw callbacks into PlcClient method calls, which in turn
    emit Qt signals.  Qt signals are thread-safe, so emitting them from the
    subscription thread is correct.

    Subscribed nodes: appDone, conv_start, awaitApp, readDone, readPresence.
    """

    def __init__(self, plc_client: "PlcClient") -> None:
        self._plc = plc_client

    def datachange_notification(self, node, val, data) -> None:  # noqa: ANN001
        """
        Route a node value change to the appropriate PlcClient method or signal.

        :param node: opcua Node object that changed.
        :param val:  New value (Python type matching the PLC data type).
        :param data: Raw MonitoredItemNotification (unused).
        """
        try:
            alias = self._plc._node_id_to_alias.get(node.nodeid)
            if alias is None:
                return  # Not a tracked node — ignore

            # Always emit generic data_changed so the GUI can display live values
            self._plc.data_changed.emit(alias, val)

            # Specific handlers for nodes that drive the production workflow.
            # Only act on rising edges (True) to avoid double-triggering.
            if alias == "appDone" and bool(val):
                # PLC finished drilling cycle → reset PLC flag, notify controller
                self._plc._on_app_done()

            elif alias == "conv_start" and bool(val):
                self._plc.conv_start.emit()

            elif alias == "awaitApp" and bool(val):
                # PLC stepped into the "await MES command" GRAPH state
                self._plc.await_app.emit()

            elif alias == "readDone" and bool(val):
                # RF210R finished reading RFID tag → decode and emit rfid_tag_read
                self._plc._on_rfid_read_done()

        except Exception as exc:  # noqa: BLE001
            self._plc.error.emit(f"Subscription handler exception: {exc}")

    def event_notification(self, event) -> None:  # noqa: ANN001
        """Required by the opcua SubHandler interface; not used here."""


# ---------------------------------------------------------------------------
# PlcClient
# ---------------------------------------------------------------------------

class PlcClient(QThread):
    """
    OPC UA client for the Siemens S7-1512SP PLC, running in a QThread.

    The worker thread (run()) connects to the PLC, sets up subscriptions, and
    runs a lightweight background loop.  Live node-change handling currently
    comes from subscriptions; the loop is a placeholder for periodic reads.
    GUI-thread code interacts with the PLC exclusively through:
      - Qt signals (received events from the PLC)
      - write_node() / read_node() / dispatch_order() (commands to the PLC)

    Signal overview
    ---------------
    connected        Emitted once after a successful OPC UA connect.
    disconnected     Emitted when the connection is lost or stop_client() called.
    rfid_tag_read    Emitted with parsed RFID payload when readDone fires.
    app_done         Emitted after the drilling cycle completes (appDone → True).
    conv_start       Emitted when the PLC raises the conveyor/start dispatch trigger.
    await_app        Emitted when the PLC enters the "await MES" GRAPH state.
    data_changed     Emitted for every subscribed node value change (node alias, value).
    error            Emitted with a human-readable message on any fault.

    Typical lifecycle
    -----------------
    1.  Instantiate once in mes_app.py.
    2.  Connect signals to controller slots.
    3.  Call start_client() — begins the worker thread.
    4.  On QApplication.aboutToQuit, call stop_client() — blocks until thread exits.
    """

    # ------------------------------------------------------------------
    # Qt signals — all thread-safe to emit from the worker thread
    # ------------------------------------------------------------------
    connected:     pyqtSignal = pyqtSignal()
    disconnected:  pyqtSignal = pyqtSignal()
    rfid_tag_read: pyqtSignal = pyqtSignal(dict)    # {"order_id": int, "task_code": int, "quantity": int}
    app_done:      pyqtSignal = pyqtSignal()
    conv_start:    pyqtSignal = pyqtSignal()
    await_app:     pyqtSignal = pyqtSignal()
    data_changed:  pyqtSignal = pyqtSignal(str, object)  # (node_alias, new_value)
    error:         pyqtSignal = pyqtSignal(str)

    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)

        self._url = PLC_URL
        self._uri = SIEMENS_URI

        # opcua objects — created inside run(), replaced on each reconnect
        self._client:            Optional[Client]       = None
        self._subscription:      Optional[object]       = None
        self._nodes:             dict[str, object]      = {}
        self._node_id_to_alias:  dict[object, str]      = {}

        # Protects dispatch_order() against concurrent calls from the GUI thread
        # and avoids interleaving writes with subscription callbacks.
        self._write_lock = threading.Lock()

        # Set by stop_client() to break the poll loop and reconnect loop
        self._stop_event = threading.Event()

        # Public connection state (read from any thread; written only in worker)
        self._connected = False
        self.last_error = ""

    # ------------------------------------------------------------------
    # Public API (called from the GUI thread)
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected

    def start_client(self) -> None:
        """
        Start the background worker thread from the GUI thread after signal
        wiring is in place.
        """
        self._stop_event.clear()
        self.start()  # invokes run() in the new thread

    def stop_client(self) -> None:
        """
        Signal the worker thread to exit and block until it has finished.
        Call during application shutdown (e.g. QApplication.aboutToQuit).
        """
        self._stop_event.set()
        self.wait()  # blocks GUI thread until QThread.run() returns

    def dispatch_order(self, order_id: int, task_code: int, quantity: int) -> bool:
        """
        Send a production order to the PLC from the GUI thread when the PLC is
        ready for a new job.

        Write sequence — DO NOT reorder these steps:

          1. taskCode   ← Siemens GRAPH latches this before sendTCPcmd fires.
                          If appRun fires first, Festo CECC gets a stale task code.
          2. writeData  ← Serialize order into 32-byte RFID payload (DB2).
          3. doWrite    ← Trigger RF210R IO-Link write to the physical RFID tag.
          4. writeDone  ← Poll until PLC confirms write (or 5 s timeout).
          5. appRun     ← Kick the GRAPH sequence; PLC sends TCP frame to Festo.

        :param order_id:  Numeric order identifier.  Must fit in uint32.
        :param task_code: Recipe code 0-3 from RECIPE_TASK_CODES.
        :param quantity:  Units to produce.  Must fit in uint32.

        """
        if not self._connected:
            self.last_error = "dispatch_order: PLC not connected — skipping"
            self.error.emit(self.last_error)
            return False

        with self._write_lock:
            try:
                # ── Step 1: task code ────────────────────────────────────────
                # The Siemens GRAPH sendTCPcmd step reads taskCode immediately
                # when it fires.  Write this before anything else.
                self._write_node("taskCode", task_code)
                logger.debug("Step 1 OK taskCode=%d", task_code)

                # ── Step 2: RFID payload ─────────────────────────────────────
                # Encode order data into 32 bytes and write to identData.writeData.
                rfid_payload = encode_rfid(order_id, task_code, quantity)
                self._write_node("writeData", rfid_payload)
                logger.debug("Step 2 OK writeData written (order_id=%d)", order_id)

                # ── Step 3: trigger RFID write ────────────────────────────────
                # Setting doWrite=True tells the PLC's rfidControl block to
                # forward writeData to the RF210R IO-Link reader/writer.
                self._write_node("doWrite", True)
                logger.debug("Step 3 OK doWrite=True, waiting for writeDone...")

                # ── Step 4: wait for PLC RFID write confirmation ──────────────
                # The PLC clears writeDone after the IO-Link cycle completes.
                # Timeout after WRITE_TIMEOUT_S — likely cause: tag not present
                # at the write station or IO-Link fault.
                deadline = time.monotonic() + WRITE_TIMEOUT_S
                write_confirmed = False
                while time.monotonic() < deadline:
                    if self._stop_event.is_set():
                        self.last_error = "dispatch_order aborted during shutdown"
                        return False
                    if self._read_bool("writeDone"):
                        write_confirmed = True
                        break
                    time.sleep(0.05)  # 50 ms poll during RFID write wait

                if not write_confirmed:
                    # Clean up doWrite before reporting failure
                    self._write_node("doWrite", False)
                    self.last_error = (
                        f"dispatch_order: writeDone timeout after {WRITE_TIMEOUT_S}s "
                        f"(order_id={order_id}). Check RFID tag at write station."
                    )
                    self.error.emit(self.last_error)
                    return False

                # Clear doWrite — PLC handshake expects this after writeDone
                self._write_node("doWrite", False)
                logger.debug("Step 4 OK writeDone received, doWrite cleared")

                # ── Step 5: start drilling cycle ──────────────────────────────
                # Only reached if taskCode and RFID write both succeeded.
                # The PLC GRAPH transitions to the drilling state, fires
                # sendTCPcmd to the Festo CECC (2-byte frame: taskCode, 0x01),
                # and eventually sets appDone=True when complete.
                self._write_node("appRun", True)
                logger.info(
                    "Step 5 OK appRun=True — drilling started "
                    "(order_id=%d task_code=%d quantity=%d)",
                    order_id, task_code, quantity,
                )
                self.last_error = ""
                return True

            except Exception as exc:  # noqa: BLE001
                self.last_error = f"dispatch_order failed: {exc}"
                self.error.emit(self.last_error)
                return False

    def write_node(self, alias: str, value: object) -> None:
        """
        Generic write to a PLC node.  Thread-safe.

        :param alias: Key from NODE_PATHS, e.g. "appRun", "release".
        :param value: Python value matching the PLC data type.
                      Bool for Bool nodes, int (0-255) for Byte, list[int] for arrays.
        """
        if not self._connected:
            self.error.emit(f"write_node({alias!r}): not connected")
            return
        try:
            with self._write_lock:
                self._write_node(alias, value)
        except Exception as exc:  # noqa: BLE001
            self.error.emit(f"write_node({alias!r}) error: {exc}")

    def read_node(self, alias: str) -> Optional[object]:
        """
        Generic read from a PLC node.  Thread-safe.

        :param alias: Key from NODE_PATHS.
        :returns:     Current node value, or None on error / not connected.
        """
        if not self._connected:
            self.error.emit(f"read_node({alias!r}): not connected")
            return None
        try:
            node = self._nodes.get(alias)
            if node is None:
                self.error.emit(f"read_node: unknown alias {alias!r}")
                return None
            return node.get_value()
        except Exception as exc:  # noqa: BLE001
            self.error.emit(f"read_node({alias!r}) error: {exc}")
            return None

    # ------------------------------------------------------------------
    # QThread worker entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Worker thread entry point — called automatically by QThread.start().

        Outer loop: connect with exponential backoff.
        Inner loop: _poll_loop() until stop event or connection error.
        """
        backoff = BACKOFF_BASE_S

        while not self._stop_event.is_set():
            try:
                self._connect()
                backoff = BACKOFF_BASE_S  # connection succeeded — reset backoff
                self._poll_loop()         # blocks until stop or exception

            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "PLC connection lost: %s. Retry in %.0fs", exc, backoff
                )
                self.error.emit(
                    f"PLC connection error: {exc}. Reconnecting in {backoff:.0f}s…"
                )
                self._cleanup()

                # Wait for backoff duration but wake immediately on stop request
                self._stop_event.wait(timeout=backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_S)

        # stop_client() was called — clean up and exit thread
        self._cleanup()
        logger.info("PlcClient worker thread exited cleanly")

    # ------------------------------------------------------------------
    # Internal — worker thread methods
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """
        Establish the OPC UA connection, resolve the Siemens namespace, build
        the node map, register subscriptions, and reset PLC control flags to a
        known-good initial state.

        Emits connected() on success.
        Raises on any OPC UA error so run() can catch and retry.
        """
        logger.info("Connecting to PLC at %s", self._url)

        client = Client(self._url)
        client.connect()

        # Resolve the Siemens S7 OPC UA namespace index.
        # The index can vary between PLC restarts so it must be queried at runtime.
        idx = client.get_namespace_index(self._uri)
        logger.info("Connected to PLC. Siemens namespace index = %d", idx)

        # Build alias → Node map and the reverse NodeId → alias map.
        # The reverse map lets the subscription handler identify nodes by object identity.
        nodes: dict[str, object] = {}
        nid_map: dict[object, str] = {}
        for alias, path in NODE_PATHS.items():
            node = client.get_node(f"ns={idx};s={path}")
            nodes[alias] = node
            nid_map[node.nodeid] = alias

        self._client           = client
        self._nodes            = nodes
        self._node_id_to_alias = nid_map
        self._connected        = True

        # Register OPC UA subscriptions.
        # The subscription runs in an opcua-internal thread; the handler safely
        # emits Qt signals from that thread (Qt cross-thread signals are queued).
        handler = OpcUaSubscriptionHandler(self)
        sub = client.create_subscription(SUBSCRIPTION_INTERVAL_MS, handler)
        sub.subscribe_data_change([
            nodes[alias] for alias in SUBSCRIBED_ALIASES
        ])
        self._subscription = sub
        logger.info(
            "Subscribed to: %s", ", ".join(sorted(SUBSCRIBED_ALIASES))
        )

        # Reset control flags to known-good state.
        # A prior session crash may have left stale True values on the PLC.
        self._write_node("appRun",  False)
        self._write_node("appDone", False)
        self._write_node("doWrite", False)
        logger.debug("Control flags reset to False")

        self.connected.emit()

    def _poll_loop(self) -> None:
        """
        100 ms background loop.  Runs after a successful _connect().

        The OPC UA subscription is the only active notification path for
        appDone, conv_start, awaitApp, readDone, and readPresence in the current code.
        This loop is intentionally idle until periodic reads are added here.

        Additional periodic reads (for example drillDone or machine mode
        display) can be added here without touching the subscription handler.

        Exits when _stop_event is set or an unhandled exception propagates
        (causing run() to catch it and attempt reconnection).
        """
        logger.debug("Poll loop started")
        while not self._stop_event.is_set():
            # Subscription handles primary event flow — poll is a backstop only.
            # Add periodic reads here as needed, e.g.:
            #   drill_done = self._read_bool("drillDone")
            #   self.data_changed.emit("drillDone", drill_done)
            time.sleep(POLL_INTERVAL_S)
        logger.debug("Poll loop exited")

    def _cleanup(self) -> None:
        """
        Tear down the OPC UA subscription and disconnect the client.
        Safe to call even if _connect() failed partway through.
        Emits disconnected() if the client was previously connected.
        """
        was_connected = self._connected
        self._connected = False

        if self._subscription is not None:
            try:
                self._subscription.delete()
            except Exception:  # noqa: BLE001
                pass  # Best-effort — already disconnecting
            self._subscription = None

        if self._client is not None:
            try:
                self._client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self._client = None

        self._nodes.clear()
        self._node_id_to_alias.clear()

        if was_connected:
            self.disconnected.emit()
            logger.info("Disconnected from PLC")

    # ------------------------------------------------------------------
    # Subscription callback handlers (called from opcua subscription thread)
    # ------------------------------------------------------------------

    def _on_rfid_read_done(self) -> None:
        """
        Handle the readDone → True transition.

        Reads the 32-byte identData.readData payload, decodes it, and emits
        rfid_tag_read with the parsed order fields.

        IMPORTANT: Do NOT reset readDone here.  The PLC's rfidControl block
        automatically clears readDone at the start of the next IO-Link read
        cycle.  Clearing it from Python causes a race condition where the PLC
        misses its own reset and re-fires the flag unexpectedly.

        """
        try:
            raw: list[int] = self._nodes["readData"].get_value()
            if not raw:
                self.error.emit("_on_rfid_read_done: readData returned empty list")
                return

            tag_data = decode_rfid(raw)
            logger.info("RFID tag decoded: %s", tag_data)
            self.rfid_tag_read.emit(tag_data)

        except Exception as exc:  # noqa: BLE001
            self.error.emit(f"_on_rfid_read_done error: {exc}")

    def _on_app_done(self) -> None:
        """
        Handle the appDone → True transition.

        Resets appDone=False on the PLC before emitting app_done.  The GRAPH
        sequence waits in the "appDone" step until the flag is cleared; if the
        MES does not clear it, the PLC stalls and cannot accept the next job.

        The app_done signal is emitted AFTER the PLC reset so the controller
        does not attempt to dispatch the next order before the PLC is ready.

        """
        try:
            # PLC handshake: clear appDone so GRAPH can advance to the idle step.
            # This MUST happen before the controller dispatches the next order.
            self._write_node("appDone", False)
            logger.info("appDone cleared on PLC, emitting app_done signal")
            self.app_done.emit()

        except Exception as exc:  # noqa: BLE001
            self.error.emit(f"_on_app_done error: {exc}")

    # ------------------------------------------------------------------
    # Low-level node I/O (no error handling — callers must handle exceptions)
    # ------------------------------------------------------------------

    def _write_node(self, alias: str, value: object) -> None:
        """
        Write a value to a PLC node.

        The variant type is queried from the node itself so that Bool, Byte,
        and Array[0..31] of Byte are all handled correctly without hard-coding
        variant types.  For Array nodes pass a list[int]; the opcua library
        maps this to the correct UA array variant.

        :raises KeyError:               If alias is not in NODE_PATHS.
        :raises ua.UaStatusCodeError:   On OPC UA write failure (bad node,
                                        bad type, PLC in error state, etc.).
        """
        node = self._nodes.get(alias)
        if node is None:
            raise KeyError(f"_write_node: unknown alias {alias!r}")

        variant_type = node.get_data_type_as_variant_type()
        node.set_value(ua.DataValue(ua.Variant(value, variant_type)))
        logger.debug("Wrote %r ← %r", alias, value)

    def _read_bool(self, alias: str) -> bool:
        """
        Read a Bool node.  Returns False on any error — safe default for
        polling conditions (e.g. writeDone polling in dispatch_order).
        """
        try:
            node = self._nodes.get(alias)
            return bool(node.get_value()) if node is not None else False
        except Exception:  # noqa: BLE001
            return False

