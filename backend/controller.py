from __future__ import annotations

from model import MesManager
from view import MesView


class MesController:
    def __init__(self, model: MesManager, view: MesView) -> None:
        self.model = model
        self.view = view
        self.current_username: str | None = None
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

    def _refresh_stations(self) -> None:
        self.view.populate_stations(self.model.list_stations())

    def _refresh_orders(self) -> None:
        self.view.populate_orders(self.model.list_orders())
