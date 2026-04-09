"""OPC UA client helpers for the Siemens PLC used by the MES app."""

from __future__ import annotations

import logging
import struct
import threading
import time
from typing import Optional

from opcua import Client, ua
from PyQt5.QtCore import QThread, pyqtSignal

logger = logging.getLogger(__name__)

# Configuration

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

# PLC node paths used by the MES.

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

# Workflow nodes watched by subscriptions and fallback polling.
SUBSCRIBED_ALIASES: frozenset[str] = frozenset({
    "appDone",
    "conv_start",
    "awaitApp",
    "readDone",
    "readPresence",
})

# Recipe to task code mapping.

RECIPE_TASK_CODES: dict[str, int] = {
    "No Holes":    0,
    "Left Holes":  1,
    "Right Holes": 2,
    "All Holes":   3,
}

# RFID payload layout:
# bytes 0-3 order_id, 4-7 task_code, 8-11 quantity, 12-31 padding.

RFID_DATA_SIZE = 32


def encode_rfid(order_id: int, task_code: int, quantity: int) -> list[int]:
    """Pack order data into the 32-byte RFID payload."""
    buf = bytearray(RFID_DATA_SIZE)
    struct.pack_into(">I", buf, 0, order_id)
    struct.pack_into(">I", buf, 4, task_code)
    struct.pack_into(">I", buf, 8, quantity)
    return list(buf)


def format_rfid_trace(order_id: int, task_code: int, quantity: int) -> str:
    """Human-readable summary of the RFID payload written to the carrier."""
    return f"dbid={order_id};task={task_code};qty={quantity}"


def decode_rfid(data: list[int]) -> dict[str, int]:
    """Read order fields back out of an RFID payload."""
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


# Subscription handler

class OpcUaSubscriptionHandler:
    """Routes OPC UA subscription callbacks into PlcClient."""

    def __init__(self, plc_client: "PlcClient") -> None:
        self._plc = plc_client

    def datachange_notification(self, node, val, data) -> None:  # noqa: ANN001
        """Route a node change to the matching PlcClient handler."""
        try:
            alias = self._plc._node_id_to_alias.get(node.nodeid)
            if alias is None:
                return  # Not a tracked node — ignore

            # Update the live node monitor in the GUI.
            self._plc.data_changed.emit(alias, val)

            # Only act on rising edges for workflow events.
            if alias == "appDone" and bool(val):
                # Reset the PLC handshake before telling the controller.
                self._plc._on_app_done()

            elif alias == "conv_start" and bool(val):
                self._plc.conv_start.emit()

            elif alias == "awaitApp" and bool(val):
                self._plc.await_app.emit()

            elif alias == "readDone" and bool(val):
                self._plc._on_rfid_read_done()

        except Exception as exc:  # noqa: BLE001
            self._plc.error.emit(f"Subscription handler exception: {exc}")

    def event_notification(self, event) -> None:  # noqa: ANN001
        """Required by the opcua SubHandler interface; not used here."""


class PlcClient(QThread):
    """Background OPC UA client used by the controller and Qt UI."""

    # Qt signals
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

    # Public API

    @property
    def is_connected(self) -> bool:
        return self._connected

    def start_client(self) -> None:
        """Start the background PLC thread."""
        self._stop_event.clear()
        self.start()  # invokes run() in the new thread

    def stop_client(self) -> None:
        """Stop the worker thread and wait for it to finish."""
        self._stop_event.set()
        self.wait()  # blocks GUI thread until QThread.run() returns

    def dispatch_order(self, order_id: int, task_code: int, quantity: int) -> bool:
        """Write task data, push the RFID payload, then start the PLC cycle."""
        if not self._connected:
            self.last_error = "dispatch_order: PLC not connected — skipping"
            self.error.emit(self.last_error)
            return False

        with self._write_lock:
            try:
                # Step 1: write task code before the PLC latches it.
                # The Siemens GRAPH sendTCPcmd step reads taskCode immediately
                # when it fires.  Write this before anything else.
                self._write_node("taskCode", task_code)
                logger.debug("Step 1 OK taskCode=%d", task_code)

                # Step 2: write the RFID payload.
                # Encode order data into 32 bytes and write to identData.writeData.
                rfid_payload = encode_rfid(order_id, task_code, quantity)
                self._write_node("writeData", rfid_payload)
                logger.debug("Step 2 OK writeData written (order_id=%d)", order_id)

                # Step 3: trigger the RFID write.
                # Setting doWrite=True tells the PLC's rfidControl block to
                # forward writeData to the RF210R IO-Link reader/writer.
                self._write_node("doWrite", True)
                logger.debug("Step 3 OK doWrite=True, waiting for writeDone...")

                # Step 4: wait for the PLC to confirm the RFID write.
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

                # Step 5: start the drilling cycle.
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
        """Thread-safe helper for writing a PLC node by alias."""
        if not self._connected:
            self.error.emit(f"write_node({alias!r}): not connected")
            return
        try:
            with self._write_lock:
                self._write_node(alias, value)
        except Exception as exc:  # noqa: BLE001
            self.error.emit(f"write_node({alias!r}) error: {exc}")

    def read_node(self, alias: str) -> Optional[object]:
        """Thread-safe helper for reading a PLC node by alias."""
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

    # Worker lifecycle

    def run(self) -> None:
        """Reconnect in the background until stopped."""
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

    # Internal helpers

    def _connect(self) -> None:
        """Connect to the PLC, resolve nodes, and start subscriptions."""
        logger.info("Connecting to PLC at %s", self._url)

        client = Client(self._url)
        client.connect()

        # The Siemens namespace index can change after PLC restarts.
        idx = client.get_namespace_index(self._uri)
        logger.info("Connected to PLC. Siemens namespace index = %d", idx)

        # Build alias -> Node and NodeId -> alias maps.
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
        handler = OpcUaSubscriptionHandler(self)
        sub = client.create_subscription(SUBSCRIPTION_INTERVAL_MS, handler)
        sub.subscribe_data_change([
            nodes[alias] for alias in SUBSCRIBED_ALIASES
        ])
        self._subscription = sub
        logger.info(
            "Subscribed to: %s", ", ".join(sorted(SUBSCRIBED_ALIASES))
        )

        # Clear control flags left behind by an earlier session.
        self._write_node("appRun",  False)
        self._write_node("appDone", False)
        self._write_node("doWrite", False)
        logger.debug("Control flags reset to False")

        self.connected.emit()

    def _poll_loop(self) -> None:
        """Fallback poll loop for PLC values that are not subscription-driven."""
        logger.debug("Poll loop started")
        while not self._stop_event.is_set():
            time.sleep(POLL_INTERVAL_S)
        logger.debug("Poll loop exited")

    def _cleanup(self) -> None:
        """Delete subscriptions and disconnect cleanly."""
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

    # Subscription callbacks

    def _on_rfid_read_done(self) -> None:
        """Decode the RFID payload and emit it to the controller."""
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
        """Clear appDone on the PLC, then notify the controller."""
        try:
            # Clear the handshake before the controller dispatches again.
            self._write_node("appDone", False)
            logger.info("appDone cleared on PLC, emitting app_done signal")
            self.app_done.emit()

        except Exception as exc:  # noqa: BLE001
            self.error.emit(f"_on_app_done error: {exc}")

    # Low-level node I/O

    def _write_node(self, alias: str, value: object) -> None:
        """Write a value to a PLC node using its native variant type."""
        node = self._nodes.get(alias)
        if node is None:
            raise KeyError(f"_write_node: unknown alias {alias!r}")

        variant_type = node.get_data_type_as_variant_type()
        node.set_value(ua.DataValue(ua.Variant(value, variant_type)))
        logger.debug("Wrote %r ← %r", alias, value)

    def _read_bool(self, alias: str) -> bool:
        """Read a boolean node and fall back to False on error."""
        try:
            node = self._nodes.get(alias)
            return bool(node.get_value()) if node is not None else False
        except Exception:  # noqa: BLE001
            return False

