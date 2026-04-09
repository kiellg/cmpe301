from __future__ import annotations

import pytest

from oee import calculate_oee


def _log_row(
    manager,
    *,
    order_id: int,
    business_order_id: str,
    recipe: str,
    actual_start: str,
    actual_end: str,
    final_status: str,
    cycle_complete: bool,
    logged_at: str,
    good_units: int,
    defect_count: int,
    fault_code: str | None = None,
    result_message: str | None = None,
) -> None:
    manager.log_process_data(
        order_id=order_id,
        business_order_id=business_order_id,
        station_id=1,
        recipe=recipe,
        actual_start=actual_start,
        actual_end=actual_end,
        final_status=final_status,
        result_message=result_message,
        fault_code=fault_code,
        cycle_complete=cycle_complete,
        logged_at=logged_at,
        good_units=good_units,
        defect_count=defect_count,
    )


def test_tc08_oee_normal_case_uses_logged_records_and_defect_counts(manager_factory):
    model = manager_factory()
    order = model.add_order("PO-OEE-1", "All Holes", 10, "operator1", priority=1)

    _log_row(
        model,
        order_id=order.id,
        business_order_id=order.order_id,
        recipe=order.recipe,
        actual_start="2026-04-08T10:00:00+00:00",
        actual_end="2026-04-08T10:01:00+00:00",
        final_status="Completed",
        cycle_complete=True,
        logged_at="2026-04-08T10:01:00+00:00",
        good_units=4,
        defect_count=1,
        result_message="Cycle complete",
    )
    _log_row(
        model,
        order_id=order.id,
        business_order_id=order.order_id,
        recipe=order.recipe,
        actual_start="2026-04-08T10:02:00+00:00",
        actual_end="2026-04-08T10:03:00+00:00",
        final_status="Completed",
        cycle_complete=True,
        logged_at="2026-04-08T10:03:00+00:00",
        good_units=5,
        defect_count=0,
        result_message="Cycle complete",
    )

    bounds = model.get_process_data_interval_bounds()
    assert bounds == ("2026-04-08T10:00:00+00:00", "2026-04-08T10:03:00+00:00")

    records = model.list_process_data_for_interval(*bounds)
    metrics = calculate_oee(records, *bounds)

    assert metrics.completed_cycles == 2
    assert metrics.failed_cycles == 0
    assert metrics.availability == pytest.approx(120 / 180)
    assert metrics.performance == pytest.approx(1.0)
    assert metrics.quality == pytest.approx(0.9)
    assert metrics.oee == pytest.approx(0.6)
    assert metrics.quality_mode == "unit_counts"


def test_tc08_oee_zero_completed_cycles_returns_zero_output_for_empty_interval(manager_factory):
    model = manager_factory()
    interval_start = "2026-04-08T11:00:00+00:00"
    interval_end = "2026-04-08T11:10:00+00:00"

    records = model.list_process_data_for_interval(interval_start, interval_end)
    metrics = calculate_oee(records, interval_start, interval_end)

    assert records == []
    assert metrics.completed_cycles == 0
    assert metrics.failed_cycles == 0
    assert metrics.availability == 0.0
    assert metrics.performance == 0.0
    assert metrics.quality == 1.0
    assert metrics.oee == 0.0
    assert metrics.quality_mode == "default_100"


def test_tc08_oee_quality_uses_explicit_failed_results_when_defects_are_not_logged(manager_factory):
    model = manager_factory()
    order = model.add_order("PO-OEE-2", "Left Holes", 2, "operator1", priority=1)

    _log_row(
        model,
        order_id=order.id,
        business_order_id=order.order_id,
        recipe=order.recipe,
        actual_start="2026-04-08T12:00:00+00:00",
        actual_end="2026-04-08T12:01:00+00:00",
        final_status="Completed",
        cycle_complete=True,
        logged_at="2026-04-08T12:01:00+00:00",
        good_units=2,
        defect_count=0,
        result_message="PLC appDone received",
    )
    _log_row(
        model,
        order_id=order.id,
        business_order_id=order.order_id,
        recipe=order.recipe,
        actual_start="2026-04-08T12:02:00+00:00",
        actual_end="2026-04-08T12:03:00+00:00",
        final_status="Failed",
        cycle_complete=False,
        logged_at="2026-04-08T12:03:00+00:00",
        good_units=0,
        defect_count=0,
        fault_code="Timeout",
        result_message="Timeout",
    )

    bounds = model.get_process_data_interval_bounds()
    records = model.list_process_data_for_interval(*bounds)
    metrics = calculate_oee(records, *bounds)

    assert metrics.completed_cycles == 1
    assert metrics.failed_cycles == 1
    assert metrics.quality == pytest.approx(0.5)
    assert metrics.quality_mode == "cycle_failure_approx"


def test_tc08_oee_quality_defaults_to_100_percent_without_failures(manager_factory):
    model = manager_factory()
    order = model.add_order("PO-OEE-3", "Right Holes", 1, "operator1", priority=1)

    _log_row(
        model,
        order_id=order.id,
        business_order_id=order.order_id,
        recipe=order.recipe,
        actual_start="2026-04-08T13:00:00+00:00",
        actual_end="2026-04-08T13:01:30+00:00",
        final_status="Completed",
        cycle_complete=True,
        logged_at="2026-04-08T13:01:30+00:00",
        good_units=1,
        defect_count=0,
        result_message="PLC appDone received",
    )

    bounds = model.get_process_data_interval_bounds()
    records = model.list_process_data_for_interval(*bounds)
    metrics = calculate_oee(records, *bounds)

    assert metrics.quality == 1.0
    assert metrics.quality_mode == "default_100"
    assert metrics.performance == pytest.approx(60 / 90)


def test_tc08_controller_refresh_orders_updates_oee_label_from_persisted_history(
    manager_factory,
    controller_factory,
    fake_view,
):
    model = manager_factory()
    order = model.add_order("PO-OEE-4", "All Holes", 1, "operator1", priority=1)
    _log_row(
        model,
        order_id=order.id,
        business_order_id=order.order_id,
        recipe=order.recipe,
        actual_start="2026-04-08T14:00:00+00:00",
        actual_end="2026-04-08T14:01:00+00:00",
        final_status="Completed",
        cycle_complete=True,
        logged_at="2026-04-08T14:01:00+00:00",
        good_units=1,
        defect_count=0,
        result_message="PLC appDone received",
    )

    controller = controller_factory(model, view=fake_view)
    controller._refresh_orders()

    assert fake_view.oee_updates
    latest = fake_view.oee_updates[-1]
    assert latest["availability"] == pytest.approx(1.0)
    assert latest["performance"] == pytest.approx(1.0)
    assert latest["quality"] == pytest.approx(1.0)
    assert "Ideal cycle: 60s" in str(latest["detail_text"])
