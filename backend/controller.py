from __future__ import annotations

import sys
from datetime import datetime, timezone

from model import MesManager, ProductionOrder
from opcua_client import RECIPE_TASK_CODES, PlcClient, format_rfid_trace
from view import MesView


class MesController:
    def __init__(self, model: MesManager, view: MesView, plc_client: PlcClient) -> None:
        self.model = model
        self.view = view
        self.plc_client = plc_client
        self.current_username: str | None = None
        self._active_order: dict | None = None
        self._dispatch_hint_order_id: int | None = None

        self._connect_signals()

    def _connect_signals(self) -> None:
        self.view.login_requested.connect(self.handle_login)
        self.view.show_register_requested.connect(self.view.show_register)
        self.view.show_login_requested.connect(self.view.show_login)
        self.view.register_requested.connect(self.handle_register)
        self.view.open_password_dialog_requested.connect(self.view.show_password_dialog)
        self.view.password_update_requested.connect(self.handle_password_update)
        self.view.logout_requested.connect(self.handle_logout)
        self.view.edit_stations_requested.connect(self.handle_edit_stations)
        self.view.reload_stations_requested.connect(self.handle_reload_stations)
        self.view.station_add_requested.connect(self.handle_add_station)
        self.view.station_edit_requested.connect(self.handle_update_station)
        self.view.station_delete_requested.connect(self.handle_delete_station)
        self.view.order_submit_requested.connect(self.handle_order_submit)
        self.view.plc_reconnect_requested.connect(self.plc_client.start_client)
        self.view.plc_manual_write.connect(self.plc_client.write_node)

    def handle_login(self, username: str, password: str) -> None:
        user = self.model.verify_credentials(username, password)
        if user is None:
            self.view.show_error("Login Failed", self.model.last_error)
            return

        self.current_username = user.username
        self.view.show_main_window(self.model.get_logged_in_user_display(user.username))
        self._refresh_orders()

    def handle_register(self, username: str, password: str, confirm_password: str) -> None:
        user = self.model.add_user(username, password, confirm_password)
        if user is None:
            self.view.show_error("Registration Failed", self.model.last_error)
            return

        self.view.show_message("Registration Successful", f"Account created for {user.username}.")
        self.view.show_login()

    def handle_password_update(
        self,
        current_password: str,
        new_password: str,
        confirm_password: str,
    ) -> None:
        if not self.current_username:
            self.view.show_error("Update Failed", "You must be logged in to update a password.")
            return

        user = self.model.update_password(
            self.current_username,
            current_password,
            new_password,
            confirm_password,
        )
        if user is None:
            self.view.show_error("Update Failed", self.model.last_error)
            return

        self.view.close_password_dialog()
        self.view.show_message("Password Updated", "Your password has been updated.")

    def handle_logout(self) -> None:
        self.current_username = None
        self.close_active_order()
        self.view.close_auxiliary_windows()
        self.view.show_login()

    def handle_edit_stations(self) -> None:
        self._refresh_stations()
        self.view.show_station_editor()

    def handle_reload_stations(self) -> None:
        self._refresh_stations()
        if self.view.station_editor.isVisible():
            self.view.show_message("Stations Reloaded", "Station data has been refreshed.")

    def handle_add_station(self, name: str, address: str, ui_file: str, enabled: bool) -> None:
        station = self.model.add_station(name, address, ui_file, enabled)
        if station is None:
            self.view.show_error("Station Save Failed", self.model.last_error)
            return

        self.view.close_station_dialog()
        self._refresh_stations()
        self.view.show_message("Station Added", f"{station.name} was added.")

    def handle_update_station(
        self,
        station_id: int,
        name: str,
        address: str,
        ui_file: str,
        enabled: bool,
    ) -> None:
        station = self.model.update_station(station_id, name, address, ui_file, enabled)
        if station is None:
            self.view.show_error("Station Update Failed", self.model.last_error)
            return

        self.view.close_station_dialog()
        self._refresh_stations()
        self.view.show_message("Station Updated", f"{station.name} was updated.")

    def handle_delete_station(self, station_id: int) -> None:
        deleted = self.model.delete_station(station_id)
        if deleted is None:
            self.view.show_error("Station Delete Failed", self.model.last_error)
            return

        self.view.close_station_dialog()
        self._refresh_stations()
        self.view.show_message("Station Deleted", "The station was removed.")

    def handle_order_submit(self, order_id: str, recipe: str, quantity: int, priority: int = 0) -> None:
        if not self.current_username:
            self.view.show_error("Order Failed", "You must be logged in to submit an order.")
            return

        order = self.model.add_order(order_id, recipe, quantity, self.current_username, priority=priority)
        if order is None:
            self.view.show_error("Order Failed", self.model.last_error)
            return

        # Keep the newly saved order as the next dispatch candidate if the PLC
        # is not ready yet.  This avoids falling back to older seeded Pending
        # orders when awaitApp eventually becomes True.
        self._dispatch_hint_order_id = order.id
        dispatched = self.dispatch_saved_order(order.id)

        self._refresh_orders()
        self.view.clear_order_form()
        if dispatched:
            message = f"Order {order.order_id} was saved and dispatched to the Siemens PLC."
        elif self._active_order is not None and self._active_order["db_id"] != order.id:
            message = f"Order {order.order_id} was saved and queued behind the active order."
        elif self.plc_client.is_connected:
            message = f"Order {order.order_id} was saved and is waiting for the PLC ready signal."
        else:
            message = f"Order {order.order_id} was saved and is waiting for the PLC connection."
        self.view.show_message("Order Submitted", message)

    def dispatch_saved_order(self, order_db_id: int) -> bool:
        order = self.model.get_order_by_id(order_db_id)
        if order is None or order.status not in {"Pending", "Dispatched"}:
            return False

        if self._active_order is not None:
            if self._active_order["db_id"] != order.id:
                queued_message = f"Queued behind order {self._active_order['order_id']}"
                self.model.update_order_traceability(order.id, last_result=queued_message)
            return False

        if not self.plc_client.is_connected:
            self.model.update_order_traceability(order.id, last_result="Waiting for PLC connection")
            return False

        await_app = self.plc_client.read_node("awaitApp")
        if not bool(await_app):
            self.model.update_order_traceability(order.id, last_result="Waiting for PLC ready signal")
            return False

        return self._dispatch_order_record(order)

    def handle_rfid_tag_read(self, payload: dict) -> None:
        order_db_id = int(payload["order_id"])
        task_code = int(payload.get("task_code", 0))
        quantity = int(payload.get("quantity", 0))

        order = self.model.get_order_by_id(order_db_id)
        if order is None:
            self.view.append_plc_log(
                f"WARNING: RFID payload db_id={order_db_id} has no matching order in SQLite"
            )
            return

        rfid_trace = format_rfid_trace(order_db_id, task_code, quantity)
        self.model.update_order_traceability(
            order_db_id,
            rfid_tag=rfid_trace,
            last_result="RFID payload detected at station",
        )

        if self._active_order is not None and self._active_order["db_id"] == order_db_id:
            self._active_order["rfid_tag"] = rfid_trace
        elif order.status == "Pending":
            self._dispatch_hint_order_id = order_db_id

        self._refresh_orders()
        self.view.update_machine_state(
            f"Carrier RFID matched order {order.order_id} - {order.recipe}"
        )
        self.view.append_plc_log(
            f"RFID matched order {order.order_id} (db_id={order_db_id})"
        )

        if (
            self._active_order is None
            and order.status == "Pending"
            and self.plc_client.is_connected
            and bool(self.plc_client.read_node("awaitApp"))
        ):
            self.dispatch_saved_order(order_db_id)

    def handle_await_app(self) -> None:
        if self._active_order is not None:
            self.view.append_plc_log(
                f"awaitApp fired while order {self._active_order['order_id']} is already active"
            )
            return

        order = self._next_dispatchable_order()
        if order is None:
            self.view.update_machine_state("Idle - no pending orders")
            self.view.append_plc_log("awaitApp fired but no pending orders are ready")
            return

        self.dispatch_saved_order(order.id)

    def handle_conv_start(self) -> None:
        if self._active_order is None:
            self.view.append_plc_log("conv_start fired but no active order tracked")
            return

        if self._active_order.get("started"):
            self.view.append_plc_log(
                f"conv_start fired again while order {self._active_order['order_id']} is already in progress"
            )
            return

        started_at = self._utc_now()
        self._active_order["started"] = True
        self._active_order["start"] = started_at
        self.model.update_order_status(
            self._active_order["db_id"],
            "In Progress",
            rfid_tag=self._active_order["rfid_tag"],
            last_result="conv_start received from PLC",
            updated_at=started_at,
        )
        self._refresh_orders()
        self.view.update_machine_state("In Progress - conveyor started")
        self.view.append_plc_log(
            f"conv_start received for order {self._active_order['order_id']} at {started_at}"
        )

    def handle_conv_end(self) -> None:
        if self._active_order is None:
            self.view.append_plc_log("conv_end fired but no active order tracked")
            return

        if not self._active_order.get("started"):
            self.view.append_plc_log(
                f"conv_end fired before conv_start for order {self._active_order['order_id']}"
            )
            return

        try:
            completed_at = self._utc_now()
            order_id = self._active_order["db_id"]
            order_label = self._active_order["order_id"]
            recipe = self._active_order["recipe"]
            quantity = self._active_order["quantity"]
            rfid_tag = self._active_order["rfid_tag"]

            self.model.update_order_status(
                order_id,
                "Completed",
                rfid_tag=rfid_tag,
                last_result="conv_end received from PLC",
                updated_at=completed_at,
            )
            self.model.log_process_data(
                order_id=order_id,
                business_order_id=order_label,
                station_id=self._default_station_id(),
                recipe=recipe,
                actual_start=self._active_order["start"],
                actual_end=completed_at,
                final_status="Completed",
                rfid_tag=rfid_tag,
                result_message="PLC conv_end received",
                cycle_complete=True,
                good_units=quantity,
                defect_count=0,
            )

            self._refresh_orders()
            self.view.update_machine_state("Completed - conveyor end reached")
            self.view.append_plc_log(
                f"conv_end received for order {order_label} at {completed_at}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"log_process_data error: {exc}", file=sys.stderr)
        finally:
            self.close_active_order()

    def handle_plc_error(self, message: str) -> None:
        print(f"[PLC ERROR] {message}", file=sys.stderr)
        self.view.append_plc_log(f"ERROR: {message}")
        self.view.update_machine_state(f"PLC error: {message[:60]}")

        if self._active_order is None:
            return

        self.model.update_order_traceability(
            self._active_order["db_id"],
            rfid_tag=self._active_order.get("rfid_tag"),
            last_result=message,
        )
        self._refresh_orders()

    def handle_plc_connected(self) -> None:
        self.view.update_machine_state("PLC connected")
        self.view.append_plc_log("Connected to PLC")

        if self._active_order is not None:
            return

        order = self._next_dispatchable_order()
        if order is not None and bool(self.plc_client.read_node("awaitApp")):
            self.dispatch_saved_order(order.id)

    def close_active_order(self) -> None:
        self._active_order = None

    def _dispatch_order_record(self, order: ProductionOrder) -> bool:
        task_code = RECIPE_TASK_CODES.get(order.recipe, 0)
        rfid_trace = format_rfid_trace(order.id, task_code, order.quantity)
        dispatched_at = self._utc_now()

        success = self.plc_client.dispatch_order(order.id, task_code, order.quantity)
        if not success:
            self.model.update_order_traceability(
                order.id,
                rfid_tag=rfid_trace,
                last_result=self.plc_client.last_error or f"Dispatch failed for order {order.order_id}",
            )
            self._refresh_orders()
            self.view.update_machine_state("Dispatch blocked - see PLC log")
            return False

        self._dispatch_hint_order_id = None
        self._active_order = {
            "db_id": order.id,
            "order_id": order.order_id,
            "recipe": order.recipe,
            "quantity": order.quantity,
            "task_code": task_code,
            "start": None,
            "dispatched_at": dispatched_at,
            "started": False,
            "rfid_tag": rfid_trace,
        }
        self.model.update_order_status(
            order.id,
            "Dispatched",
            rfid_tag=rfid_trace,
            last_result="Dispatched to Siemens PLC - waiting for conv_start",
            updated_at=dispatched_at,
        )

        self._refresh_orders()
        self.view.update_machine_state(
            f"Dispatched order {order.order_id} - waiting for conv_start"
        )
        self.view.append_plc_log(
            f"dispatch_order: db_id={order.id} order_id={order.order_id} "
            f"task_code={task_code} qty={order.quantity}"
        )
        return True

    def _next_dispatchable_order(self) -> ProductionOrder | None:
        if self._dispatch_hint_order_id is not None:
            hinted_order = self.model.get_order_by_id(self._dispatch_hint_order_id)
            if hinted_order is not None and hinted_order.status == "Pending":
                return hinted_order
            self._dispatch_hint_order_id = None

        for order in self.model.list_orders():
            if order.status == "Pending":
                return order
        return None

    def _default_station_id(self) -> int | None:
        stations = self.model.list_stations()
        for station in stations:
            if station.enabled:
                return station.id
        return stations[0].id if stations else None

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _refresh_stations(self) -> None:
        self.view.populate_stations(self.model.list_stations())

    def _refresh_orders(self) -> None:
        self.view.populate_orders(self.model.list_orders())
