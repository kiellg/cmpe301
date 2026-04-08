from __future__ import annotations

from pathlib import Path
import sys
from uuid import uuid4

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1] / "backend"
TEST_RUNTIME_DIR = Path(__file__).resolve().parents[1] / "test_runtime"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from controller import MesController
from model import MesManager


class FakeView:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.errors: list[tuple[str, str]] = []
        self.machine_states: list[str] = []
        self.logs: list[str] = []
        self.order_snapshots: list[list[object]] = []
        self.station_snapshots: list[list[object]] = []
        self.cleared_order_form = False

    def show_message(self, title: str, message: str) -> None:
        self.messages.append((title, message))

    def show_error(self, title: str, message: str) -> None:
        self.errors.append((title, message))

    def populate_orders(self, orders: list[object]) -> None:
        self.order_snapshots.append(list(orders))

    def populate_stations(self, stations: list[object]) -> None:
        self.station_snapshots.append(list(stations))

    def clear_order_form(self) -> None:
        self.cleared_order_form = True

    def update_machine_state(self, text: str) -> None:
        self.machine_states.append(text)

    def append_plc_log(self, message: str) -> None:
        self.logs.append(message)


class FakePlc:
    def __init__(
        self,
        *,
        connected: bool = False,
        await_app: bool = False,
        dispatch_result: bool = True,
        last_error: str = "",
    ) -> None:
        self.is_connected = connected
        self.await_app = await_app
        self.dispatch_result = dispatch_result
        self.last_error = last_error
        self.dispatch_calls: list[tuple[int, int, int]] = []
        self.read_calls: list[str] = []
        self.write_calls: list[tuple[str, object]] = []

    def read_node(self, alias: str) -> object:
        self.read_calls.append(alias)
        if alias == "awaitApp":
            return self.await_app
        return None

    def dispatch_order(self, order_id: int, task_code: int, quantity: int) -> bool:
        self.dispatch_calls.append((order_id, task_code, quantity))
        return self.dispatch_result

    def write_node(self, alias: str, value: object) -> None:
        self.write_calls.append((alias, value))


@pytest.fixture
def workspace_tmp_path():
    test_dir = TEST_RUNTIME_DIR / uuid4().hex
    test_dir.mkdir(parents=True, exist_ok=True)
    return test_dir


@pytest.fixture
def manager_factory(workspace_tmp_path):
    def build(name: str = "mes.db") -> MesManager:
        return MesManager(workspace_tmp_path / name)

    return build


@pytest.fixture
def fake_view() -> FakeView:
    return FakeView()


@pytest.fixture
def fake_plc_factory():
    def build(**kwargs) -> FakePlc:
        return FakePlc(**kwargs)

    return build


@pytest.fixture
def controller_factory():
    def build(model: MesManager, *, view: FakeView | None = None, plc: FakePlc | None = None) -> MesController:
        controller = MesController.__new__(MesController)
        controller.model = model
        controller.view = view or FakeView()
        controller.plc_client = plc or FakePlc()
        controller.current_username = None
        controller._active_order = None
        controller._dispatch_hint_order_id = None
        return controller

    return build
