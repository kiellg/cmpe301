"""TC-01 tests for Python-side MES order creation and queue/history behavior."""

from __future__ import annotations

import sqlite3


def test_tc01_order_creation_is_saved_and_visible_in_history(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify controller/model order submission stores the order and refreshes history."""
    model = manager_factory()
    plc = fake_plc_factory(connected=False)
    controller = controller_factory(model, view=fake_view, plc=plc)
    controller.current_username = "operator1"

    controller.handle_order_submit("PO-1001", "Left Holes", 2, priority=4)

    orders = model.list_orders()
    assert len(orders) == 1
    order = orders[0]
    assert order.order_id == "PO-1001"
    assert order.recipe == "Left Holes"
    assert order.quantity == 2
    assert order.priority == 4
    assert order.created_by == "operator1"
    assert order.status == "Pending"
    assert order.last_result == "Waiting for PLC connection"
    assert fake_view.cleared_order_form is True
    assert fake_view.messages[-1][0] == "Order Submitted"
    assert fake_view.order_snapshots[-1][0].order_id == "PO-1001"


def test_tc01_queue_ordering_matches_priority_then_newest_first(manager_factory):
    """Verify the current queue sort is priority first, then newest timestamp first."""
    model = manager_factory()
    low = model.add_order("PO-LOW", "No Holes", 1, "tester", priority=3)
    newer_same_priority = model.add_order("PO-NEWER", "Left Holes", 1, "tester", priority=5)
    older_same_priority = model.add_order("PO-OLDER", "Right Holes", 1, "tester", priority=5)

    with sqlite3.connect(model.db_path) as connection:
        connection.execute(
            "UPDATE production_orders SET created_at = ?, updated_at = ? WHERE id = ?",
            ("2026-04-08T10:00:00+00:00", "2026-04-08T10:00:00+00:00", older_same_priority.id),
        )
        connection.execute(
            "UPDATE production_orders SET created_at = ?, updated_at = ? WHERE id = ?",
            ("2026-04-08T10:00:05+00:00", "2026-04-08T10:00:05+00:00", newer_same_priority.id),
        )
        connection.execute(
            "UPDATE production_orders SET created_at = ?, updated_at = ? WHERE id = ?",
            ("2026-04-08T09:59:00+00:00", "2026-04-08T09:59:00+00:00", low.id),
        )
        connection.commit()

    ordered = model.list_orders()
    assert [order.order_id for order in ordered] == ["PO-NEWER", "PO-OLDER", "PO-LOW"]


def test_tc01_newly_saved_order_is_hint_for_next_dispatch_when_plc_not_ready(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify the controller prefers the newly saved pending order as the next hint."""
    model = manager_factory()
    model.add_order("PO-EXISTING", "All Holes", 1, "seed", priority=10)
    plc = fake_plc_factory(connected=True, await_app=False)
    controller = controller_factory(model, view=fake_view, plc=plc)
    controller.current_username = "operator1"

    controller.handle_order_submit("PO-NEW", "No Holes", 1, priority=1)

    next_order = controller._next_dispatchable_order()
    assert next_order is not None
    assert next_order.order_id == "PO-NEW"
