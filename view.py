from __future__ import annotations

from pathlib import Path

from PyQt5 import uic
from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QDialog,
    QHeaderView,
    QMessageBox,
    QTableWidgetItem,
)

from model import ProductionOrder, Station


class MesView(QObject):
    login_requested = pyqtSignal(str, str)
    show_register_requested = pyqtSignal()
    show_login_requested = pyqtSignal()
    register_requested = pyqtSignal(str, str, str)
    open_password_dialog_requested = pyqtSignal()
    password_update_requested = pyqtSignal(str, str, str)
    logout_requested = pyqtSignal()
    edit_stations_requested = pyqtSignal()
    reload_stations_requested = pyqtSignal()
    station_add_requested = pyqtSignal(str, str, str, bool)
    station_edit_requested = pyqtSignal(int, str, str, str, bool)
    station_delete_requested = pyqtSignal(int)
    order_submit_requested = pyqtSignal(str, str, int, int)

    def __init__(self) -> None:
        super().__init__()
        self.ui_dir = Path(__file__).resolve().parent
        self.station_dialog: QDialog | None = None

        self.login_window = self._load_ui("login_window.ui")
        self.register_window = self._load_ui("register_window.ui")
        self.password_dialog = self._load_ui("password_dialog.ui")
        self.station_editor = self._load_ui("station_editor.ui")
        self.main_window = self._load_ui("mes_window.ui")

        self._configure_widgets()
        self._connect_signals()

    def show_login(self) -> None:
        self.register_window.hide()
        self.main_window.hide()
        self.close_auxiliary_windows()
        self.clear_login_form()
        self.login_window.show()
        self.login_window.raise_()
        self.login_window.activateWindow()

    def show_register(self) -> None:
        self.login_window.hide()
        self.clear_register_form()
        self.register_window.show()
        self.register_window.raise_()
        self.register_window.activateWindow()

    def show_main_window(self, logged_in_text: str) -> None:
        self.login_window.hide()
        self.register_window.hide()
        self.main_window.logged_in_user_label.setText(logged_in_text)
        self.main_window.show()
        self.main_window.raise_()
        self.main_window.activateWindow()

    def show_password_dialog(self) -> None:
        self.password_dialog.current_password.clear()
        self.password_dialog.new_password.clear()
        self.password_dialog.confirm_new_password.clear()
        self.password_dialog.exec_()

    def close_password_dialog(self) -> None:
        if self.password_dialog.isVisible():
            self.password_dialog.accept()

    def show_station_editor(self) -> None:
        self.station_editor.show()
        self.station_editor.raise_()
        self.station_editor.activateWindow()

    def close_auxiliary_windows(self) -> None:
        self.close_password_dialog()
        self.close_station_dialog()
        self.station_editor.hide()

    def close_station_dialog(self) -> None:
        if self.station_dialog is not None and self.station_dialog.isVisible():
            self.station_dialog.accept()

    def populate_stations(self, stations: list[Station]) -> None:
        table = self.station_editor.stations_table
        table.setRowCount(len(stations))
        for row_index, station in enumerate(stations):
            name_item = QTableWidgetItem(station.name)
            name_item.setData(Qt.UserRole, station.id)
            address_item = QTableWidgetItem(station.address)
            ui_file_item = QTableWidgetItem(station.ui_file)
            status_item = QTableWidgetItem("Enabled" if station.enabled else "Disabled")

            table.setItem(row_index, 0, name_item)
            table.setItem(row_index, 1, address_item)
            table.setItem(row_index, 2, ui_file_item)
            table.setItem(row_index, 3, status_item)

        table.resizeRowsToContents()

    def populate_orders(self, orders: list[ProductionOrder]) -> None:
        table = self.main_window.orders_table
        table.setRowCount(len(orders))
        status_colours = {
            "In Progress": QColor(255, 255, 200),
            "Completed":   QColor(200, 255, 200),
            "Failed":      QColor(255, 200, 200),
        }
        for row_index, order in enumerate(orders):
            row_colour = status_colours.get(order.status)
            items = [
                QTableWidgetItem(order.order_id),
                QTableWidgetItem(order.recipe),
                QTableWidgetItem(str(order.quantity)),
                QTableWidgetItem(order.status),
                QTableWidgetItem(order.created_by or ""),
                QTableWidgetItem(order.created_at),
                QTableWidgetItem(str(order.priority)),
                QTableWidgetItem(order.rfid_tag if order.rfid_tag is not None else "--"),
            ]
            for col_index, item in enumerate(items):
                if row_colour is not None:
                    item.setBackground(row_colour)
                table.setItem(row_index, col_index, item)

        table.resizeRowsToContents()

    def clear_order_form(self) -> None:
        self.main_window.order_id_input.clear()
        self.main_window.quantity_spin.setValue(1)
        self.main_window.priority_spin.setValue(1)
        if self.main_window.recipe_combo.count() > 0:
            self.main_window.recipe_combo.setCurrentIndex(0)

    def show_message(self, title: str, message: str) -> None:
        QMessageBox.information(self._message_parent(), title, message)

    def show_error(self, title: str, message: str) -> None:
        QMessageBox.warning(self._message_parent(), title, message)

    def _connect_signals(self) -> None:
        self.login_window.login_btn.clicked.connect(self._emit_login_requested)
        self.login_window.register_btn.clicked.connect(self.show_register_requested.emit)

        self.register_window.register_btn.clicked.connect(self._emit_register_requested)
        self.register_window.back_btn.clicked.connect(self.register_window.reject)
        self.register_window.rejected.connect(self.show_login_requested.emit)

        self.password_dialog.update_password_btn.clicked.connect(self._emit_password_update_requested)
        self.password_dialog.cancel_btn.clicked.connect(self.password_dialog.reject)

        self.main_window.logout_action.triggered.connect(self.logout_requested.emit)
        self.main_window.update_password_action.triggered.connect(
            self.open_password_dialog_requested.emit
        )
        self.main_window.edit_stations_action.triggered.connect(self.edit_stations_requested.emit)
        self.main_window.reload_stations_action.triggered.connect(
            self.reload_stations_requested.emit
        )
        self.main_window.submit_order_btn.clicked.connect(self._emit_order_submit_requested)

        self.station_editor.add_btn.clicked.connect(self._open_add_station_dialog)
        self.station_editor.edit_btn.clicked.connect(self._open_selected_station_dialog)
        self.station_editor.remove_btn.clicked.connect(self._request_selected_station_delete)
        self.station_editor.close_btn.clicked.connect(self.station_editor.hide)
        self.station_editor.stations_table.itemDoubleClicked.connect(
            self._open_selected_station_dialog
        )

    def _configure_widgets(self) -> None:
        self.login_window.password.setEchoMode(self.login_window.password.Password)
        self.register_window.password.setEchoMode(self.register_window.password.Password)
        self.register_window.confirm_password.setEchoMode(
            self.register_window.confirm_password.Password
        )
        self.password_dialog.current_password.setEchoMode(
            self.password_dialog.current_password.Password
        )
        self.password_dialog.new_password.setEchoMode(self.password_dialog.new_password.Password)
        self.password_dialog.confirm_new_password.setEchoMode(
            self.password_dialog.confirm_new_password.Password
        )

        self.main_window.quantity_spin.setMinimum(1)
        self.main_window.priority_spin.setMinimum(1)
        self.main_window.priority_spin.setMaximum(10)
        self._configure_table(self.station_editor.stations_table)
        self._configure_table(self.main_window.orders_table)

        if self.main_window.recipe_combo.count() == 0:
            self.main_window.recipe_combo.addItems(
                ["No Holes", "Left Holes", "Right Holes", "All Holes"]
            )

    def _configure_table(self, table) -> None:
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

    def _emit_login_requested(self) -> None:
        self.login_requested.emit(
            self.login_window.username.text(),
            self.login_window.password.text(),
        )

    def _emit_register_requested(self) -> None:
        self.register_requested.emit(
            self.register_window.username.text(),
            self.register_window.password.text(),
            self.register_window.confirm_password.text(),
        )

    def _emit_password_update_requested(self) -> None:
        self.password_update_requested.emit(
            self.password_dialog.current_password.text(),
            self.password_dialog.new_password.text(),
            self.password_dialog.confirm_new_password.text(),
        )

    def _emit_order_submit_requested(self) -> None:
        self.order_submit_requested.emit(
            self.main_window.order_id_input.text(),
            self.main_window.recipe_combo.currentText(),
            int(self.main_window.quantity_spin.value()),
            int(self.main_window.priority_spin.value()),
        )

    def _open_add_station_dialog(self) -> None:
        self._show_station_dialog()

    def _open_selected_station_dialog(self, *_args) -> None:
        station = self._selected_station()
        if station is None:
            self.show_error("Station Selection", "Select a station first.")
            return

        self._show_station_dialog(station)

    def _request_selected_station_delete(self) -> None:
        station = self._selected_station()
        if station is None:
            self.show_error("Station Selection", "Select a station first.")
            return

        self.station_delete_requested.emit(station.id)

    def _show_station_dialog(self, station: Station | None = None) -> None:
        self.station_dialog = self._load_ui("station_dialog.ui")

        self.station_dialog.station_name.setText("" if station is None else station.name)
        self.station_dialog.address_input.setText("" if station is None else station.address)
        self.station_dialog.ui_file_input.setText("" if station is None else station.ui_file)
        self.station_dialog.enabled_checkbox.setChecked(True if station is None else station.enabled)

        self.station_dialog.delete_station_btn.setVisible(station is not None)
        self.station_dialog.cancel_btn.clicked.connect(self.station_dialog.reject)
        self.station_dialog.apply_btn.clicked.connect(
            lambda: self._emit_station_dialog_requested(station)
        )

        if station is None:
            self.station_dialog.setWindowTitle("Add Station")
        else:
            self.station_dialog.setWindowTitle("Edit Station")
            self.station_dialog.delete_station_btn.clicked.connect(
                lambda: self.station_delete_requested.emit(station.id)
            )

        self.station_dialog.exec_()
        self.station_dialog = None

    def _emit_station_dialog_requested(self, station: Station | None) -> None:
        if self.station_dialog is None:
            return

        name = self.station_dialog.station_name.text()
        address = self.station_dialog.address_input.text()
        ui_file = self.station_dialog.ui_file_input.text()
        enabled = self.station_dialog.enabled_checkbox.isChecked()

        if station is None:
            self.station_add_requested.emit(name, address, ui_file, enabled)
            return

        self.station_edit_requested.emit(station.id, name, address, ui_file, enabled)

    def _selected_station(self) -> Station | None:
        table = self.station_editor.stations_table
        current_row = table.currentRow()
        if current_row < 0:
            return None

        name_item = table.item(current_row, 0)
        address_item = table.item(current_row, 1)
        ui_file_item = table.item(current_row, 2)
        status_item = table.item(current_row, 3)
        if name_item is None or address_item is None or ui_file_item is None or status_item is None:
            return None

        return Station(
            id=int(name_item.data(Qt.UserRole)),
            name=name_item.text(),
            address=address_item.text(),
            ui_file=ui_file_item.text(),
            enabled=status_item.text().strip().lower() == "enabled",
        )

    def _message_parent(self):
        return self.station_dialog or QApplication.activeWindow() or self.main_window

    def clear_login_form(self) -> None:
        self.login_window.username.clear()
        self.login_window.password.clear()

    def clear_register_form(self) -> None:
        self.register_window.username.clear()
        self.register_window.password.clear()
        self.register_window.confirm_password.clear()

    def _load_ui(self, file_name: str):
        return uic.loadUi(str(self.ui_dir / file_name))
