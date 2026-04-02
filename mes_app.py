from __future__ import annotations

import sys

from PyQt5.QtWidgets import QApplication

from controller import MesController
from model import MesManager
from view import MesView


def create_app(db_path: str = "mes.db") -> tuple[QApplication, MesManager, MesView, MesController]:
    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("MES Access Manager")

    model = MesManager(db_path)
    view = MesView()
    controller = MesController(model, view)
    return app, model, view, controller


def main() -> int:
    app, _model, view, _controller = create_app()
    view.show_login()
    return app.exec_()


if __name__ == "__main__":
    raise SystemExit(main())
