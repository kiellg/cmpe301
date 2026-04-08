"""TC-10 tests for simulated disconnect, timeout, and graceful degradation paths."""

from __future__ import annotations

from opcua_client import PlcClient


class _SequencedBoolNode:
    """Minimal fake OPC UA node that returns a scripted sequence of boolean values."""

    def __init__(self, values: list[bool]) -> None:
        self._values = list(values)
        self._index = 0

    def get_value(self) -> bool:
        if self._index >= len(self._values):
            return self._values[-1]
        value = self._values[self._index]
        self._index += 1
        return value


def test_tc10_controller_keeps_active_order_non_failed_on_simulated_plc_error(
    manager_factory,
    controller_factory,
    fake_view,
):
    """Verify controller error handling logs the issue without auto-failing the active order."""
    model = manager_factory()
    model.add_station("Drilling", "172.21.3.1", "", True)
    order = model.add_order("PO-FAIL", "Left Holes", 2, "operator1", priority=1)
    model.update_order_status(order.id, "In Progress", last_result="conv_start received from PLC")
    controller = controller_factory(model, view=fake_view)
    controller._active_order = {
        "db_id": order.id,
        "order_id": order.order_id,
        "recipe": order.recipe,
        "quantity": order.quantity,
        "started": True,
        "start": "2026-04-08T12:00:00+00:00",
        "rfid_tag": f"dbid={order.id};task=1;qty=2",
    }

    controller.handle_plc_error("dispatch_order: writeDone timeout after 5s")

    updated = model.get_order_by_id(order.id)
    history = model.list_process_data(order.id)
    assert updated is not None
    assert updated.status == "In Progress"
    assert updated.last_result == "dispatch_order: writeDone timeout after 5s"
    assert controller._active_order is not None
    assert history == []
    assert fake_view.machine_states[-1].startswith("PLC error:")


def test_tc10_plc_client_run_emits_reconnect_error_when_connect_fails(monkeypatch):
    """Verify PlcClient emits reconnect errors and cleans up when _connect() fails."""
    plc = PlcClient()
    errors: list[str] = []
    cleanup_calls: list[str] = []
    plc.error.connect(errors.append)

    def fake_connect() -> None:
        plc._stop_event.set()
        raise RuntimeError("boom")

    monkeypatch.setattr(plc, "_connect", fake_connect)
    monkeypatch.setattr(plc, "_cleanup", lambda: cleanup_calls.append("cleanup"))

    plc.run()

    assert cleanup_calls == ["cleanup", "cleanup"]
    assert any("PLC connection error: boom" in message for message in errors)


def test_tc10_poll_loop_recovers_missed_conv_end_subscription(monkeypatch):
    """Verify fallback polling emits conv_end when the PLC end tag changes without a subscription event."""
    plc = PlcClient()
    completions: list[str] = []
    sleep_calls = {"count": 0}

    plc._nodes = {
        "appDone": _SequencedBoolNode([False, False]),
        "awaitApp": _SequencedBoolNode([False, False]),
        "conv_start": _SequencedBoolNode([False, False]),
        "conv_end": _SequencedBoolNode([False, True]),
        "readDone": _SequencedBoolNode([False, False]),
        "readPresence": _SequencedBoolNode([False, False]),
    }
    plc.conv_end.connect(lambda: completions.append("done"))

    def fake_sleep(_seconds: float) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 2:
            plc._stop_event.set()

    monkeypatch.setattr("opcua_client.time.sleep", fake_sleep)

    plc._poll_loop()

    assert completions == ["done"]
