from __future__ import annotations

import sys

from PyQt5.QtWidgets import QApplication

from controller import MesController
from model import MesManager
from plc_client import PlcClient
from view import MesView


def create_app(
    db_path: str = "mes.db",
) -> tuple[QApplication, MesManager, MesView, MesController, PlcClient]:
    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("MES Access Manager")

    model = MesManager(db_path)
    view = MesView()

    # PlcClient runs its OPC UA polling loop in a background QThread.
    # It must be created before MesController so the controller can store
    # a reference to it and call dispatch_order() from its handlers.
    plc_client = PlcClient()

    controller = MesController(model, view, plc_client)

    # Wire PlcClient → controller.  All signals are queued cross-thread
    # connections: the PlcClient worker thread emits, the Qt event loop
    # delivers to the GUI thread, so no explicit locking is needed here.
    plc_client.rfid_tag_read.connect(controller.handle_rfid_tag_read)
    plc_client.app_done.connect(controller.handle_app_done)
    plc_client.await_app.connect(controller.handle_await_app)
    plc_client.error.connect(controller.handle_plc_error)
    plc_client.connected.connect(controller.handle_plc_connected)
    plc_client.data_changed.connect(view.update_node_monitor)
    plc_client.data_changed.connect(
        lambda alias, val: view.append_plc_log(f"{alias} = {val}")
    )
    plc_client.connected.connect(lambda: view.update_plc_status(True))
    plc_client.disconnected.connect(lambda: view.update_plc_status(False))
    plc_client.error.connect(lambda msg: view.append_plc_log(f"ERROR: {msg}"))

    # Graceful shutdown: stop the PLC worker thread before Qt tears down
    # its event loop.  stop_client() sets the threading.Event and calls
    # QThread.wait(), so it blocks until the worker exits cleanly.
    app.aboutToQuit.connect(plc_client.stop_client)

    plc_client.start_client()

    return app, model, view, controller, plc_client


def main() -> int:
    app, _model, view, _controller, _plc = create_app()
    view.show_login()
    return app.exec_()


if __name__ == "__main__":
    raise SystemExit(main())
