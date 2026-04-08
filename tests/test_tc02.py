"""TC-02 tests for simulated Python-side OPC UA dispatch behavior."""

from __future__ import annotations

from plc_client import PlcClient, RECIPE_TASK_CODES, encode_rfid


def test_tc02_dispatch_order_writes_expected_python_side_sequence(monkeypatch):
    """Verify dispatch_order writes the expected node sequence without a real PLC."""
    plc = PlcClient()
    plc._connected = True
    writes: list[tuple[str, object]] = []

    monkeypatch.setattr(plc, "_write_node", lambda alias, value: writes.append((alias, value)))
    monkeypatch.setattr(plc, "_read_bool", lambda alias: alias == "writeDone")

    success = plc.dispatch_order(7, RECIPE_TASK_CODES["All Holes"], 5)

    assert success is True
    assert writes == [
        ("taskCode", 3),
        ("writeData", encode_rfid(7, 3, 5)),
        ("doWrite", True),
        ("doWrite", False),
        ("appRun", True),
    ]


def test_tc02_controller_dispatches_saved_order_with_simulated_plc(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify controller dispatch updates order state using a simulated PLC client."""
    model = manager_factory()
    order = model.add_order("PO-2001", "Right Holes", 4, "operator1", priority=2)
    plc = fake_plc_factory(connected=True, await_app=True, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)

    dispatched = controller.dispatch_saved_order(order.id)

    assert dispatched is True
    assert plc.dispatch_calls == [(order.id, RECIPE_TASK_CODES["Right Holes"], 4)]
    updated = model.get_order_by_id(order.id)
    assert updated is not None
    assert updated.status == "In Progress"
    assert updated.rfid_tag == f"dbid={order.id};task=2;qty=4"
    assert updated.last_result == "Dispatched to Siemens PLC"
    assert controller._active_order["task_code"] == 2
    assert fake_view.machine_states[-1] == f"Dispatched order {order.order_id} - {order.recipe}"
