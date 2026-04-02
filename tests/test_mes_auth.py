from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from model import MesManager


TEST_DB_DIR = Path(__file__).resolve().parents[1] / "test_runtime"
TEST_DB_DIR.mkdir(exist_ok=True)


def build_manager():
    db_path = TEST_DB_DIR / f"{uuid4().hex}.db"
    return MesManager(db_path)


def test_register_success():
    manager = build_manager()

    user = manager.add_user("operator1", "secret123", "secret123")

    assert user is not None
    assert user.username == "operator1"


def test_duplicate_user_rejected():
    manager = build_manager()
    assert manager.add_user("operator1", "secret123", "secret123") is not None

    duplicate = manager.add_user("operator1", "different", "different")

    assert duplicate is None


def test_login_success():
    manager = build_manager()
    manager.add_user("operator1", "secret123", "secret123")

    user = manager.verify_credentials("operator1", "secret123")

    assert user is not None
    assert user.username == "operator1"


def test_login_fail():
    manager = build_manager()
    manager.add_user("operator1", "secret123", "secret123")

    user = manager.verify_credentials("operator1", "wrong-password")

    assert user is None


def test_update_password_success():
    manager = build_manager()
    manager.add_user("operator1", "secret123", "secret123")

    updated = manager.update_password("operator1", "secret123", "newpass456", "newpass456")

    assert updated is not None
    assert manager.verify_credentials("operator1", "newpass456") is not None


def test_update_password_wrong_current_password_rejected():
    manager = build_manager()
    manager.add_user("operator1", "secret123", "secret123")

    updated = manager.update_password("operator1", "bad-current", "newpass456", "newpass456")

    assert updated is None


def test_add_station_success():
    manager = build_manager()

    station = manager.add_station("Mixer", "192.168.0.10", "mixer.ui", True)

    assert station is not None
    assert station.name == "Mixer"
    assert len(manager.list_stations()) == 1


def test_duplicate_station_rejected():
    manager = build_manager()
    assert manager.add_station("Mixer", "192.168.0.10", "mixer.ui", True) is not None

    duplicate = manager.add_station("Mixer", "192.168.0.11", "backup.ui", False)

    assert duplicate is None


def test_add_order_success():
    manager = build_manager()

    order = manager.add_order("PO-1001", "Recipe Alpha", 25, "operator1")

    assert order is not None
    assert order.status == "Pending"
    assert len(manager.list_orders()) == 1


def test_invalid_order_quantity_rejected():
    manager = build_manager()

    order = manager.add_order("PO-1001", "Recipe Alpha", 0, "operator1")

    assert order is None
