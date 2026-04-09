from __future__ import annotations

from datetime import datetime
from pathlib import Path

from PyQt5 import uic
from PyQt5.QtCore import QObject, Qt, QTimer, pyqtSignal
from PyQt5.QtGui import QColor, QFontDatabase, QTextCursor
from PyQt5.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QComboBox,
    QDialog,
    QFormLayout,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from model import ProductionOrder, Station
from opcua_client import encode_rfid, RECIPE_TASK_CODES


class WriteTagDialog(QDialog):
    def __init__(self, view: "MesView") -> None:
        super().__init__(view.main_window)
        self._view = view
        self._waiting_for_write = False
        self._tick_count = 0
        self._signal_connected = True
        self._pending_order_id = 0
        self._pending_recipe = ""

        self.setModal(True)
        self.setWindowTitle("Write RFID Tag")

        self._timer = QTimer(self)
        self._timer.setInterval(100)
        self._timer.timeout.connect(self._handle_timeout_tick)

        self._order_id_spin = QSpinBox(self)
        self._order_id_spin.setRange(1, 9999)

        self._recipe_combo = QComboBox(self)
        self._recipe_combo.addItems(list(RECIPE_TASK_CODES.keys()))

        self._quantity_spin = QSpinBox(self)
        self._quantity_spin.setRange(1, 100)

        self._write_button = QPushButton("Write", self)
        self._cancel_button = QPushButton("Cancel", self)

        form_layout = QFormLayout()
        form_layout.addRow("Order ID", self._order_id_spin)
        form_layout.addRow("Recipe", self._recipe_combo)
        form_layout.addRow("Quantity", self._quantity_spin)

        buttons_layout = QHBoxLayout()
        buttons_layout.addStretch()
        buttons_layout.addWidget(self._write_button)
        buttons_layout.addWidget(self._cancel_button)

        root_layout = QVBoxLayout(self)
        root_layout.addLayout(form_layout)
        root_layout.addLayout(buttons_layout)

        self._write_button.clicked.connect(self._start_write)
        self._cancel_button.clicked.connect(self.reject)
        self._view.plc_data_changed.connect(self._handle_plc_data_changed)

    def _start_write(self) -> None:
        order_id = int(self._order_id_spin.value())
        recipe = self._recipe_combo.currentText()
        quantity = int(self._quantity_spin.value())
        task_code = RECIPE_TASK_CODES[recipe]
        encoded = encode_rfid(order_id, task_code, quantity)

        self._pending_order_id = order_id
        self._pending_recipe = recipe
        self._waiting_for_write = True
        self._tick_count = 0

        self._write_button.setText("Writing...")
        self._write_button.setEnabled(False)
        self._cancel_button.setEnabled(False)

        self._view.plc_manual_write.emit("writeData", encoded)
        self._view.plc_manual_write.emit("doWrite", True)
        self._timer.start()

    def _handle_plc_data_changed(self, alias: str, value: object) -> None:
        if not self._waiting_for_write:
            return
        if alias != "writeDone" or not bool(value):
            return

        self._waiting_for_write = False
        self._timer.stop()
        self._view.plc_manual_write.emit("doWrite", False)
        self._view.append_plc_log(
            f"Tag written: order #{self._pending_order_id} ({self._pending_recipe})"
        )
        self.accept()

    def _handle_timeout_tick(self) -> None:
        if not self._waiting_for_write:
            return

        self._tick_count += 1
        if self._tick_count < 50:
            return

        self._waiting_for_write = False
        self._timer.stop()
        self._view.append_plc_log("ERROR: writeDone timeout — is carrier at stopper?")
        self._view.plc_manual_write.emit("doWrite", False)
        self.reject()

    def reject(self) -> None:
        if self._waiting_for_write:
            return
        super().reject()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._waiting_for_write:
            event.ignore()
            return
        self._disconnect_signal()
        super().closeEvent(event)

    def done(self, result: int) -> None:
        self._timer.stop()
        self._disconnect_signal()
        super().done(result)

    def _disconnect_signal(self) -> None:
        if not self._signal_connected:
            return
        try:
            self._view.plc_data_changed.disconnect(self._handle_plc_data_changed)
        except TypeError:
            pass
        self._signal_connected = False


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
    plc_reconnect_requested = pyqtSignal()
    plc_manual_write = pyqtSignal(str, object)
    plc_data_changed = pyqtSignal(str, object)

    def __init__(self) -> None:
        super().__init__()
        self.ui_dir = Path(__file__).resolve().parent
        self.station_dialog: QDialog | None = None
        self._node_rows: dict[str, int] = {}
        self._plc_buttons: list[QPushButton] = []

        self.login_window = self._load_ui("login_window.ui")
        self.register_window = self._load_ui("register_window.ui")
        self.password_dialog = self._load_ui("password_dialog.ui")
        self.station_editor = self._load_ui("station_editor.ui")
        self.main_window = self._load_ui("mes_window.ui")

        self._plc_dot: QLabel | None = None
        self._plc_status_label: QLabel | None = None
        self._plc_connection_state_label: QLabel | None = None
        self._plc_machine_state_label: QLabel | None = None
        self._node_table: QTableWidget | None = None
        self._plc_log: QPlainTextEdit | None = None

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
            "Completed": QColor(200, 255, 200),
            "Failed": QColor(255, 200, 200),
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
                QTableWidgetItem(order.last_result if order.last_result is not None else "--"),
                QTableWidgetItem(order.updated_at if order.updated_at is not None else "--"),
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

    def update_machine_state(self, text: str) -> None:
        machine_state_text = f"Machine State: {text}"
        self.main_window.machine_state_label.setText(machine_state_text)
        if self._plc_machine_state_label is not None:
            self._plc_machine_state_label.setText(machine_state_text)

    def update_oee(
        self,
        availability: float,
        performance: float,
        quality: float,
        *,
        detail_text: str | None = None,
    ) -> None:
        oee = availability * performance * quality
        self.main_window.oee_label.setText(
            f"OEE: {oee:.1%}  |  "
            f"A: {availability:.1%}  "
            f"P: {performance:.1%}  "
            f"Q: {quality:.1%}"
        )
        if detail_text:
            self.main_window.monitoring_label.setText(detail_text)

    def show_oee_unavailable(self, message: str) -> None:
        self.main_window.oee_label.setText(f"OEE: -- ({message})")
        self.main_window.monitoring_label.setText(message)

    def update_node_monitor(self, alias: str, value: object) -> None:
        if alias in self._node_rows and self._node_table is not None:
            row = self._node_rows[alias]
            display = value
            if alias == "readData" and isinstance(value, (list, tuple, bytes, bytearray)):
                display = " ".join(f"{int(byte):02X}" for byte in list(value)[:12])
            self._node_table.setItem(row, 2, QTableWidgetItem(str(display)))
            self._node_table.setItem(
                row,
                3,
                QTableWidgetItem(datetime.now().strftime("%H:%M:%S")),
            )
        self.plc_data_changed.emit(alias, value)

    def update_plc_status(self, connected: bool) -> None:
        if self._plc_dot is None or self._plc_status_label is None:
            return

        color = "#2ecc71" if connected else "#e74c3c"
        state = "Connected" if connected else "Disconnected"
        self._plc_dot.setStyleSheet(f"color: {color}; font-size: 16px;")
        self._plc_status_label.setText(f"Status: {state}")
        if self._plc_connection_state_label is not None:
            self._plc_connection_state_label.setText(f"PLC Connection: {state}")
        for button in self._plc_buttons:
            button.setEnabled(connected)

    def append_plc_log(self, message: str) -> None:
        if self._plc_log is None:
            return

        timestamp = datetime.now().strftime("%H:%M:%S")
        self._plc_log.appendPlainText(f"[{timestamp}] {message}")
        while self._plc_log.document().lineCount() > 200:
            cursor = self._plc_log.textCursor()
            cursor.movePosition(QTextCursor.Start)
            cursor.select(QTextCursor.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()
        self._plc_log.verticalScrollBar().setValue(
            self._plc_log.verticalScrollBar().maximum()
        )

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
        self.main_window.orders_table.setColumnCount(10)
        self.main_window.orders_table.setHorizontalHeaderLabels(
            [
                "Order ID",
                "Recipe",
                "Quantity",
                "Status",
                "Created By",
                "Created At",
                "Priority",
                "RFID Tag",
                "Last Result",
                "Last Update",
            ]
        )

        if self.main_window.recipe_combo.count() == 0:
            self.main_window.recipe_combo.addItems(
                ["No Holes", "Left Holes", "Right Holes", "All Holes"]
            )

        self._build_plc_diagnostics_tab()
        self.update_machine_state("Not connected")
        self.update_plc_status(False)
        self.show_oee_unavailable("OEE will appear after process history is logged.")

    def _build_plc_diagnostics_tab(self) -> None:
        plc_tab = QWidget(self.main_window)
        plc_layout = QVBoxLayout(plc_tab)

        status_layout = QHBoxLayout()
        self._plc_dot = QLabel("●", plc_tab)
        self._plc_status_label = QLabel("Status: Disconnected", plc_tab)
        reconnect_button = QPushButton("Reconnect", plc_tab)
        reconnect_button.clicked.connect(self.plc_reconnect_requested.emit)
        status_layout.addWidget(self._plc_dot)
        status_layout.addWidget(self._plc_status_label)
        status_layout.addWidget(reconnect_button)
        status_layout.addStretch()
        plc_layout.addLayout(status_layout)

        self._node_table = QTableWidget(plc_tab)
        self._node_table.setColumnCount(4)
        self._node_table.setHorizontalHeaderLabels(
            ["Node Alias", "DB", "Value", "Last Updated"]
        )
        self._configure_table(self._node_table)
        rows = [
            ("appRun", "DB1"),
            ("appDone", "DB1"),
            ("conv_start", "Default"),
            ("awaitApp", "DB1"),
            ("drillDone", "DB1"),
            ("taskCode", "DB1"),
            ("doRead", "DB3"),
            ("readDone", "DB3"),
            ("readPresence", "DB3"),
            ("doWrite", "DB3"),
            ("writeDone", "DB3"),
            ("readData", "DB2"),
        ]
        self._node_table.setRowCount(len(rows))
        for row_index, (alias, db_name) in enumerate(rows):
            self._node_rows[alias] = row_index
            self._node_table.setItem(row_index, 0, QTableWidgetItem(alias))
            self._node_table.setItem(row_index, 1, QTableWidgetItem(db_name))
            self._node_table.setItem(row_index, 2, QTableWidgetItem("--"))
            self._node_table.setItem(row_index, 3, QTableWidgetItem("--"))
        plc_layout.addWidget(self._node_table)

        controls_layout = QHBoxLayout()

        manual_group = QGroupBox("Manual PLC Controls", plc_tab)
        manual_layout = QVBoxLayout(manual_group)
        write_tag_button = QPushButton("Write Tag", manual_group)
        read_tag_button = QPushButton("Read RFID Tag", manual_group)
        reset_app_run_button = QPushButton("Reset appRun", manual_group)
        reset_app_done_button = QPushButton("Reset appDone", manual_group)
        write_tag_button.clicked.connect(self._open_write_tag_dialog)
        read_tag_button.clicked.connect(self._trigger_manual_rfid_read)
        reset_app_run_button.clicked.connect(
            lambda: self.plc_manual_write.emit("appRun", False)
        )
        reset_app_done_button.clicked.connect(
            lambda: self.plc_manual_write.emit("appDone", False)
        )
        for button in [
            write_tag_button,
            read_tag_button,
            reset_app_run_button,
            reset_app_done_button,
        ]:
            self._plc_buttons.append(button)
            manual_layout.addWidget(button)
        manual_layout.addStretch()

        status_group = QGroupBox("PLC Status", plc_tab)
        status_group_layout = QVBoxLayout(status_group)
        self._plc_machine_state_label = QLabel("Machine State: Not connected", status_group)
        self._plc_machine_state_label.setWordWrap(True)
        self._plc_connection_state_label = QLabel(
            "PLC Connection: Disconnected",
            status_group,
        )
        status_group_layout.addWidget(self._plc_machine_state_label)
        status_group_layout.addWidget(self._plc_connection_state_label)
        status_group_layout.addStretch()

        controls_layout.addWidget(manual_group)
        controls_layout.addWidget(status_group)
        plc_layout.addLayout(controls_layout)

        self._plc_log = QPlainTextEdit(plc_tab)
        self._plc_log.setReadOnly(True)
        self._plc_log.setLineWrapMode(QPlainTextEdit.NoWrap)
        self._plc_log.setFont(QFontDatabase.systemFont(QFontDatabase.FixedFont))
        plc_layout.addWidget(self._plc_log)

        self.main_window.main_tabs.addTab(plc_tab, "PLC Diagnostics")

    def _trigger_manual_rfid_read(self) -> None:
        self.plc_manual_write.emit("doRead", True)
        self.append_plc_log("Manual RFID read triggered")

    def _open_write_tag_dialog(self) -> None:
        WriteTagDialog(self).exec_()

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
