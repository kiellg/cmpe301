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
    """Verify dispatch hands the order to the PLC and waits for conv_start before In Progress."""
    model = manager_factory()
    order = model.add_order("PO-2001", "Right Holes", 4, "operator1", priority=2)
    plc = fake_plc_factory(connected=True, await_app=True, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)

    dispatched = controller.dispatch_saved_order(order.id)

    assert dispatched is True
    assert plc.dispatch_calls == [(order.id, RECIPE_TASK_CODES["Right Holes"], 4)]
    updated = model.get_order_by_id(order.id)
    assert updated is not None
    assert updated.status == "Dispatched"
    assert updated.rfid_tag == f"dbid={order.id};task=2;qty=4"
    assert updated.last_result == "Dispatched to Siemens PLC - waiting for conv_start"
    assert controller._active_order["task_code"] == 2
    assert controller._active_order["started"] is False
    assert fake_view.machine_states[-1] == f"Dispatched order {order.order_id} - waiting for conv_start"


def test_tc02_controller_marks_order_in_progress_on_conv_start(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify conv_start is the Python-side trigger that moves a dispatched order to In Progress."""
    model = manager_factory()
    order = model.add_order("PO-2002", "Left Holes", 1, "operator1", priority=1)
    plc = fake_plc_factory(connected=True, await_app=True, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)

    assert controller.dispatch_saved_order(order.id) is True

    controller.handle_conv_start()

    updated = model.get_order_by_id(order.id)
    assert updated is not None
    assert updated.status == "In Progress"
    assert updated.last_result == "conv_start received from PLC"
    assert controller._active_order["started"] is True
    assert controller._active_order["start"] is not None
    assert fake_view.machine_states[-1] == "In Progress - conveyor started"


def test_tc02_controller_completes_if_conv_end_is_already_high_when_start_arrives(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify the controller reconciles to Completed if conv_end is already high at conv_start time."""
    model = manager_factory()
    model.add_station("Drilling", "172.21.3.1", "", True)
    order = model.add_order("PO-2003", "All Holes", 2, "operator1", priority=1)
    plc = fake_plc_factory(connected=True, await_app=True, dispatch_result=True)
    original_read_node = plc.read_node

    def read_node(alias: str) -> object:
        if alias == "conv_end":
            return True
        return original_read_node(alias)

    plc.read_node = read_node
    controller = controller_factory(model, view=fake_view, plc=plc)

    assert controller.dispatch_saved_order(order.id) is True

    controller.handle_conv_start()

    updated = model.get_order_by_id(order.id)
    history = model.list_process_data(order.id)
    assert updated is not None
    assert updated.status == "Completed"
    assert updated.last_result == "conv_end received from PLC"
    assert controller._active_order is None
    assert history[0]["final_status"] == "Completed"
