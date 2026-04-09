"""TC-07 tests for Python-side logging, history, and restart persistence."""

from __future__ import annotations

from model import MesManager


def test_tc07_logging_handle_app_done_records_completed_history(
    manager_factory,
    controller_factory,
    fake_view,
):
    """Verify completion logging writes both order status and process history rows."""
    model = manager_factory()
    model.add_station("Drilling", "172.21.3.1", "", True)
    order = model.add_order("PO-3001", "All Holes", 3, "operator1", priority=3)
    controller = controller_factory(model, view=fake_view)
    controller._active_order = {
        "db_id": order.id,
        "order_id": order.order_id,
        "recipe": order.recipe,
        "quantity": order.quantity,
        "start": "2026-04-08T10:00:00+00:00",
        "rfid_tag": f"dbid={order.id};task=3;qty=3",
    }

    controller.handle_app_done()

    updated = model.get_order_by_id(order.id)
    history = model.list_process_data(order.id)
    assert updated is not None
    assert updated.status == "Completed"
    assert updated.last_result == "Completed at drilling station"
    assert history[0]["final_status"] == "Completed"
    assert history[0]["actual_start"] == "2026-04-08T10:00:00+00:00"
    assert history[0]["result_message"] == "PLC appDone received"
    assert history[0]["good_units"] == 3
    assert history[0]["defect_count"] == 0


def test_tc07_logging_persists_history_after_reopening_same_database(workspace_tmp_path):
    """Verify persisted order and process history survive reopening the same DB file."""
    db_path = workspace_tmp_path / "persistent_mes.db"
    manager = MesManager(db_path)
    order = manager.add_order("PO-3002", "Left Holes", 2, "operator1", priority=1)
    manager.update_order_status(
        order.id,
        "Failed",
        last_result="Simulated timeout",
        updated_at="2026-04-08T11:00:00+00:00",
    )
    manager.log_process_data(
        order_id=order.id,
        business_order_id=order.order_id,
        station_id=1,
        recipe=order.recipe,
        actual_start="2026-04-08T10:55:00+00:00",
        actual_end="2026-04-08T11:00:00+00:00",
        final_status="Failed",
        result_message="Simulated timeout",
        fault_code="Simulated timeout",
        logged_at="2026-04-08T11:00:00+00:00",
        good_units=0,
        defect_count=0,
    )

    reopened = MesManager(db_path)
    persisted_order = reopened.get_order_by_id(order.id)
    persisted_history = reopened.list_process_data(order.id)

    assert persisted_order is not None
    assert persisted_order.status == "Failed"
    assert persisted_order.last_result == "Simulated timeout"
    assert persisted_history == [
        {
            "id": 1,
            "order_id": order.id,
            "business_order_id": "PO-3002",
            "station_id": 1,
            "recipe": "Left Holes",
            "rfid_tag": None,
            "actual_start": "2026-04-08T10:55:00+00:00",
            "actual_end": "2026-04-08T11:00:00+00:00",
            "final_status": "Failed",
            "result_message": "Simulated timeout",
            "fault_code": "Simulated timeout",
            "cycle_complete": 0,
            "logged_at": "2026-04-08T11:00:00+00:00",
            "good_units": 0,
            "defect_count": 0,
        }
    ]
