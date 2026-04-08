"""TC-07 tests for Python-side RFID payload handling and traceability storage."""

from __future__ import annotations

from model import MesManager
from plc_client import decode_rfid, encode_rfid, format_rfid_trace


def test_tc07_rfid_python_payload_round_trip_and_trace_format():
    """Verify Python RFID helpers round-trip payload data and format trace text."""
    payload = encode_rfid(12, 2, 9)

    assert len(payload) == 32
    assert decode_rfid(payload) == {"order_id": 12, "task_code": 2, "quantity": 9}
    assert format_rfid_trace(12, 2, 9) == "dbid=12;task=2;qty=9"


def test_tc07_rfid_tag_read_updates_traceability_fields_in_database(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify a simulated RFID read updates the linked MES order traceability fields."""
    model = manager_factory()
    order = model.add_order("PO-RFID", "All Holes", 2, "operator1", priority=1)
    plc = fake_plc_factory(connected=False)
    controller = controller_factory(model, view=fake_view, plc=plc)

    controller.handle_rfid_tag_read(
        {"order_id": order.id, "task_code": 3, "quantity": 2}
    )

    reopened = MesManager(model.db_path)
    updated = reopened.get_order_by_id(order.id)
    assert updated is not None
    assert updated.rfid_tag == format_rfid_trace(order.id, 3, 2)
    assert updated.last_result == "RFID payload detected at station"
    assert reopened.list_orders()[0].rfid_tag == updated.rfid_tag
    assert fake_view.machine_states[-1] == f"Carrier RFID matched order {order.order_id} - {order.recipe}"
    assert fake_view.logs[-1] == f"RFID matched order {order.order_id} (db_id={order.id})"
    assert plc.dispatch_calls == []
