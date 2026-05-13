"""TC-09 tests for Python-side recipe and drilling variant handling."""

from __future__ import annotations

from opcua_client import RECIPE_TASK_CODES


def test_tc09_supported_recipe_mapping_matches_current_python_codes():
    """Verify the built-in drilling variants map to the task codes defined in Python."""
    assert RECIPE_TASK_CODES == {
        "No Holes": 0,
        "Left Holes": 1,
        "Right Holes": 2,
        "All Holes": 3,
    }


def test_tc09_valid_recipe_is_stored_and_dispatched_with_expected_task_code(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify a supported recipe is stored and dispatched with its mapped task code."""
    model = manager_factory()
    order = model.add_order("PO-VARIANT", "Right Holes", 4, "operator1", priority=2)
    plc = fake_plc_factory(connected=True, await_app=True, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)

    dispatched = controller.dispatch_saved_order(order.id)

    assert dispatched is True
    assert plc.dispatch_calls == [(order.id, 2, 4)]
    assert model.get_order_by_id(order.id).recipe == "Right Holes"


def test_tc09_unknown_recipe_currently_falls_back_to_task_code_zero(
    manager_factory,
    controller_factory,
    fake_view,
    fake_plc_factory,
):
    """Verify unknown recipes currently fall back to task code 0 during dispatch."""
    model = manager_factory()
    order = model.add_order("PO-UNKNOWN", "Recipe Alpha", 2, "operator1", priority=1)
    plc = fake_plc_factory(connected=True, await_app=True, dispatch_result=True)
    controller = controller_factory(model, view=fake_view, plc=plc)

    dispatched = controller.dispatch_saved_order(order.id)

    assert dispatched is True
    assert plc.dispatch_calls == [(order.id, 0, 2)]
    assert model.get_order_by_id(order.id).recipe == "Recipe Alpha"
