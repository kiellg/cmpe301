"""TC-02 tests for simulated Python-side OPC UA dispatch behavior."""

from __future__ import annotations

from opcua_client import PlcClient, RECIPE_TASK_CODES, encode_rfid


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


def test_tc02_controller_preloads_task_code_when_conv_start_fires(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify conv_start rising edge preloads the next pending order task code."""
    model = manager_factory()
    order = model.add_order("PO-2002", "All Holes", 3, "operator1", priority=2)
    plc = fake_plc_factory(connected=True, conv_start=True, await_app=False, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)

    controller.handle_conv_start()

    updated = model.get_order_by_id(order.id)

    assert plc.dispatch_calls == []
    assert plc.write_calls == [("taskCode", RECIPE_TASK_CODES["All Holes"])]
    assert updated is not None
    assert updated.status == "Pending"
    assert updated.last_result == "Task code 3 preloaded on conv_start"
    assert fake_view.machine_states[-1] == f"Preloaded taskCode for order {order.order_id} - {order.recipe}"


def test_tc02_conv_start_completes_active_order_and_starts_next_order(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify a new pallet at conv_start closes the active order and starts the next one."""
    model = manager_factory()
    model.add_station("Drilling", "172.21.3.1", "", True)
    active_order = model.add_order("PO-ACTIVE", "Left Holes", 2, "operator1", priority=2)
    next_order = model.add_order("PO-NEXT", "All Holes", 1, "operator1", priority=1)
    model.update_order_status(
        active_order.id,
        "In Progress",
        rfid_tag=f"dbid={active_order.id};task=1;qty=2",
        last_result="Dispatched to Siemens PLC",
        updated_at="2026-04-08T15:00:00+00:00",
    )
    plc = fake_plc_factory(connected=True, conv_start=True, await_app=True, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)
    controller._active_order = {
        "db_id": active_order.id,
        "order_id": active_order.order_id,
        "recipe": active_order.recipe,
        "quantity": active_order.quantity,
        "task_code": RECIPE_TASK_CODES["Left Holes"],
        "start": "2026-04-08T15:00:00+00:00",
        "rfid_tag": f"dbid={active_order.id};task=1;qty=2",
    }

    controller.handle_conv_start()

    completed = model.get_order_by_id(active_order.id)
    started = model.get_order_by_id(next_order.id)
    history = model.list_process_data(active_order.id)

    assert completed is not None
    assert completed.status == "Completed"
    assert completed.last_result == "Completed on conv_start handoff"
    assert history[0]["final_status"] == "Completed"
    assert history[0]["result_message"] == "Completed when next pallet reached conv_start (demo handoff)"
    assert started is not None
    assert started.status == "In Progress"
    assert plc.dispatch_calls == [(next_order.id, RECIPE_TASK_CODES["All Holes"], 1)]
    assert controller._active_order["db_id"] == next_order.id
