"""TC-10 tests for simulated disconnect, timeout, and graceful degradation paths."""

from __future__ import annotations

from opcua_client import PlcClient


def test_tc10_controller_marks_active_order_failed_on_simulated_plc_error(
    manager_factory,
    controller_factory,
    fake_view,
):
    """Verify controller error handling marks the active order failed and logs history."""
    model = manager_factory()
    model.add_station("Drilling", "172.21.3.1", "", True)
    order = model.add_order("PO-FAIL", "Left Holes", 2, "operator1", priority=1)
    controller = controller_factory(model, view=fake_view)
    controller._active_order = {
        "db_id": order.id,
        "order_id": order.order_id,
        "recipe": order.recipe,
        "quantity": order.quantity,
        "start": "2026-04-08T12:00:00+00:00",
        "rfid_tag": f"dbid={order.id};task=1;qty=2",
    }

    controller.handle_plc_error("dispatch_order: writeDone timeout after 5s")

    updated = model.get_order_by_id(order.id)
    history = model.list_process_data(order.id)
    assert updated is not None
    assert updated.status == "Failed"
    assert updated.last_result == "dispatch_order: writeDone timeout after 5s"
    assert controller._active_order is None
    assert history[0]["final_status"] == "Failed"
    assert history[0]["fault_code"] == "dispatch_order: writeDone timeout after 5s"
    assert fake_view.machine_states[-1] == "Execution failed - see PLC log"


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
