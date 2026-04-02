from __future__ import annotations

import sys
from datetime import datetime, timezone

from model import MesManager
from plc_client import RECIPE_TASK_CODES, PlcClient
from view import MesView


_DRILL_STATION_ID = 3


class MesController:
    def __init__(self, model: MesManager, view: MesView, plc_client: PlcClient) -> None:
        self.model = model
        self.view = view
        self.plc_client = plc_client
        self.current_username: str | None = None
        self._active_order: dict | None = None

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

        self._refresh_orders()
        self.view.clear_order_form()
        self.view.show_message("Order Submitted", f"Order {order.order_id} was saved.")

    def handle_rfid_tag_read(self, payload: dict) -> None:
        order_id = payload["order_id"]
        orders = self.model.list_orders()
        order = next(
            (
                row
                for row in orders
                if int(row.id) == order_id and row.status == "Pending"
            ),
            None,
        )
        if order is None:
            self.view.append_plc_log(
                f"WARNING: tag order #{order_id} has no matching Pending order in DB"
            )
            return

        task_code = RECIPE_TASK_CODES.get(order.recipe, 0)
        self._active_order = {
            "db_id": order_id,
            "task_code": task_code,
            "quantity": order.quantity,
            "start": datetime.now(timezone.utc).isoformat(),
        }
        self.model.update_order_status(order_id, "In Progress")
        self._refresh_orders()
        self.view.update_machine_state(
            f"Carrier arrived: order #{order_id} — {order.recipe}"
        )
        self.view.append_plc_log(
            f"RFID matched: order #{order_id} ({order.recipe})"
        )

    def handle_await_app(self) -> None:
        if self._active_order is None:
            self.view.update_machine_state("Idle — no pending orders")
            self.view.append_plc_log("awaitApp fired but no active order")
            return

        order_id = self._active_order["db_id"]
        task_code = self._active_order["task_code"]
        quantity = self._active_order["quantity"]
        try:
            self.plc_client.dispatch_order(order_id, task_code, quantity)
            self.view.update_machine_state(
                f"Dispatched order #{order_id} — drilling starting"
            )
            self.view.append_plc_log(
                f"dispatch_order: id={order_id} task_code={task_code} qty={quantity}"
            )
        except Exception as e:
            self.view.append_plc_log(f"ERROR dispatch_order: {e}")
            self.view.update_machine_state(f"Dispatch failed: {e}")

    def handle_app_done(self) -> None:
        if self._active_order is None:
            self.view.append_plc_log("appDone fired but no active order tracked")
            return

        try:
            actual_end = datetime.now(timezone.utc).isoformat()
            order_id = self._active_order["db_id"]
            self.model.update_order_status(order_id, "Completed")
            try:
                self.model.log_process_data(
                    order_id=order_id,
                    station_id=_DRILL_STATION_ID,
                    actual_start=self._active_order["start"],
                    actual_end=actual_end,
                    good_units=1,
                    defect_count=0,
                    # TODO D1: replace good_units/defect_count when quality tracking added
                )
            except Exception as e:
                print(f"log_process_data error: {e}", file=sys.stderr)

            self._refresh_orders()
            self.view.update_machine_state("Completed — ready for next carrier")
            self.view.append_plc_log(
                f"Order #{order_id} completed at {actual_end}"
            )
        finally:
            self._active_order = None

    def handle_plc_error(self, message: str) -> None:
        print(f"[PLC ERROR] {message}", file=sys.stderr)
        self.view.append_plc_log(f"ERROR: {message}")
        self.view.update_machine_state(f"PLC error: {message[:60]}")

    def handle_plc_connected(self) -> None:
        self.view.update_machine_state("PLC connected")
        self.view.append_plc_log("Connected to PLC")

    def _refresh_stations(self) -> None:
        self.view.populate_stations(self.model.list_stations())

    def _refresh_orders(self) -> None:
        self.view.populate_orders(self.model.list_orders())
