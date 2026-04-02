from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3


@dataclass
class User:
    id: int
    username: str
    password: str


@dataclass
class Station:
    id: int
    name: str
    address: str
    ui_file: str
    enabled: bool


@dataclass
class ProductionOrder:
    id: int
    order_id: str
    recipe: str
    quantity: int
    status: str
    created_by: str | None
    created_at: str
    priority: int
    rfid_tag: str | None


class MesManager:
    def __init__(self, db_path: str | Path = "mes.db") -> None:
        self.db_path = Path(db_path)
        self.last_error = ""
        self.initialize_database()

    def initialize_database(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stations (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    address TEXT NOT NULL,
                    ui_file TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS production_orders (
                    id INTEGER PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    recipe TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'Pending',
                    created_by TEXT,
                    created_at TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    rfid_tag TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS process_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    station_id INTEGER,
                    actual_start TEXT,
                    actual_end TEXT,
                    good_units INTEGER DEFAULT 1,
                    defect_count INTEGER DEFAULT 0
                )
                """
            )

            # ── production_orders migrations ──────────────────────────────────
            po_cols = self._table_columns(connection, "production_orders")
            if "priority" not in po_cols:
                cursor.execute(
                    "ALTER TABLE production_orders ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
                )
            if "rfid_tag" not in po_cols:
                cursor.execute(
                    "ALTER TABLE production_orders ADD COLUMN rfid_tag TEXT"
                )

            # ── process_data migrations ───────────────────────────────────────
            # Adds the columns needed by the new log_process_data signature.
            # Existing rows keep NULL for new columns; NOT NULL is not required
            # for added columns in SQLite ALTER TABLE.
            pd_cols = self._table_columns(connection, "process_data")
            if "actual_start" not in pd_cols:
                cursor.execute(
                    "ALTER TABLE process_data ADD COLUMN actual_start TEXT"
                )
            if "actual_end" not in pd_cols:
                cursor.execute(
                    "ALTER TABLE process_data ADD COLUMN actual_end TEXT"
                )
            if "good_units" not in pd_cols:
                cursor.execute(
                    "ALTER TABLE process_data ADD COLUMN good_units INTEGER DEFAULT 1"
                )
            if "defect_count" not in pd_cols:
                cursor.execute(
                    "ALTER TABLE process_data ADD COLUMN defect_count INTEGER DEFAULT 0"
                )
            if "station_id" not in pd_cols:
                cursor.execute(
                    "ALTER TABLE process_data ADD COLUMN station_id INTEGER"
                )

            connection.commit()

    # ── user management ───────────────────────────────────────────────────────

    def get_user_by_username(self, username: str) -> User | None:
        normalized_username = username.strip()
        if not normalized_username:
            self.last_error = "Username cannot be blank."
            return None

        with self._connect() as connection:
            row = connection.execute(
                "SELECT id, username, password FROM users WHERE lower(username) = lower(?)",
                (normalized_username,),
            ).fetchone()

        if row is None:
            self.last_error = "User not found."
            return None

        self.last_error = ""
        return self._row_to_user(row)

    def verify_credentials(self, username: str, password: str) -> User | None:
        normalized_username = username.strip()
        if not normalized_username or not password:
            self.last_error = "Username and password are required."
            return None

        user = self.get_user_by_username(normalized_username)
        if user is None:
            self.last_error = "Invalid username or password."
            return None

        if user.password != password:
            self.last_error = "Invalid username or password."
            return None

        self.last_error = ""
        return user

    def add_user(self, username: str, password: str, confirm_password: str) -> User | None:
        normalized_username = username.strip()
        if not normalized_username or not password:
            self.last_error = "Username and password cannot be blank."
            return None

        if password != confirm_password:
            self.last_error = "Passwords do not match."
            return None

        if self._username_exists(normalized_username):
            self.last_error = "That username is already registered."
            return None

        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO users (username, password) VALUES (?, ?)",
                    (normalized_username, password),
                )
                connection.commit()
                row = connection.execute(
                    "SELECT id, username, password FROM users WHERE id = ?",
                    (cursor.lastrowid,),
                ).fetchone()
        except sqlite3.IntegrityError:
            self.last_error = "That username is already registered."
            return None

        self.last_error = ""
        return self._row_to_user(row)

    def update_password(
        self,
        username: str,
        current_password: str,
        new_password: str,
        confirm_password: str,
    ) -> User | None:
        normalized_username = username.strip()
        if not normalized_username:
            self.last_error = "Username is required."
            return None

        if not current_password or not new_password:
            self.last_error = "Current and new passwords cannot be blank."
            return None

        if new_password != confirm_password:
            self.last_error = "New passwords do not match."
            return None

        user = self.verify_credentials(normalized_username, current_password)
        if user is None:
            self.last_error = "Current password is incorrect."
            return None

        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE users SET password = ? WHERE lower(username) = lower(?)",
                (new_password, normalized_username),
            )
            connection.commit()

        if cursor.rowcount == 0:
            self.last_error = "Unable to update the password."
            return None

        self.last_error = ""
        return self.get_user_by_username(normalized_username)

    # ── station management ────────────────────────────────────────────────────

    def list_stations(self) -> list[Station]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, name, address, ui_file, enabled
                FROM stations
                ORDER BY lower(name), id
                """
            ).fetchall()

        self.last_error = ""
        return [self._row_to_station(row) for row in rows]

    def add_station(self, name: str, address: str, ui_file: str, enabled: bool) -> Station | None:
        normalized_name = name.strip()
        normalized_address = address.strip()
        normalized_ui_file = ui_file.strip()

        if not normalized_name or not normalized_address:
            self.last_error = "Station name and address cannot be blank."
            return None

        if self._station_name_exists(normalized_name):
            self.last_error = "That station name is already in use."
            return None

        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO stations (name, address, ui_file, enabled)
                    VALUES (?, ?, ?, ?)
                    """,
                    (normalized_name, normalized_address, normalized_ui_file, int(bool(enabled))),
                )
                connection.commit()
                row = connection.execute(
                    """
                    SELECT id, name, address, ui_file, enabled
                    FROM stations
                    WHERE id = ?
                    """,
                    (cursor.lastrowid,),
                ).fetchone()
        except sqlite3.IntegrityError:
            self.last_error = "That station name is already in use."
            return None

        self.last_error = ""
        return self._row_to_station(row)

    def update_station(
        self,
        station_id: int,
        name: str,
        address: str,
        ui_file: str,
        enabled: bool,
    ) -> Station | None:
        normalized_name = name.strip()
        normalized_address = address.strip()
        normalized_ui_file = ui_file.strip()

        if not normalized_name or not normalized_address:
            self.last_error = "Station name and address cannot be blank."
            return None

        if self._station_name_exists(normalized_name, exclude_station_id=station_id):
            self.last_error = "That station name is already in use."
            return None

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE stations
                SET name = ?, address = ?, ui_file = ?, enabled = ?
                WHERE id = ?
                """,
                (normalized_name, normalized_address, normalized_ui_file, int(bool(enabled)), station_id),
            )
            connection.commit()
            row = connection.execute(
                """
                SELECT id, name, address, ui_file, enabled
                FROM stations
                WHERE id = ?
                """,
                (station_id,),
            ).fetchone()

        if cursor.rowcount == 0 or row is None:
            self.last_error = "Station not found."
            return None

        self.last_error = ""
        return self._row_to_station(row)

    def delete_station(self, station_id: int) -> bool | None:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM stations WHERE id = ?", (station_id,))
            connection.commit()

        if cursor.rowcount == 0:
            self.last_error = "Station not found."
            return None

        self.last_error = ""
        return True

    # ── production order management ───────────────────────────────────────────

    def add_order(
        self,
        order_id: str,
        recipe: str,
        quantity: int,
        created_by: str,
        priority: int = 0,
        rfid_tag: str | None = None,
    ) -> ProductionOrder | None:
        normalized_order_id = order_id.strip()
        normalized_recipe = recipe.strip()
        normalized_created_by = created_by.strip() if created_by else None

        if not normalized_order_id or not normalized_recipe:
            self.last_error = "Order ID and recipe cannot be blank."
            return None

        try:
            numeric_quantity = int(quantity)
        except (TypeError, ValueError):
            self.last_error = "Quantity must be a positive integer."
            return None

        if numeric_quantity <= 0:
            self.last_error = "Quantity must be greater than zero."
            return None

        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO production_orders
                    (order_id, recipe, quantity, status, created_by, created_at, priority, rfid_tag)
                VALUES (?, ?, ?, 'Pending', ?, ?, ?, ?)
                """,
                (
                    normalized_order_id,
                    normalized_recipe,
                    numeric_quantity,
                    normalized_created_by,
                    created_at,
                    int(priority),
                    rfid_tag,
                ),
            )
            connection.commit()
            row = connection.execute(
                """
                SELECT id, order_id, recipe, quantity, status, created_by, created_at,
                       priority, rfid_tag
                FROM production_orders
                WHERE id = ?
                """,
                (cursor.lastrowid,),
            ).fetchone()

        self.last_error = ""
        return self._row_to_order(row)

    def list_orders(self) -> list[ProductionOrder]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, order_id, recipe, quantity, status, created_by, created_at,
                       priority, rfid_tag
                FROM production_orders
                ORDER BY priority DESC, created_at DESC, id DESC
                """
            ).fetchall()

        self.last_error = ""
        return [self._row_to_order(row) for row in rows]

    def update_order_status(self, order_pk: int, status: str) -> bool:
        """
        Update the status of a production order by its integer primary key.

        :param order_pk: The production_orders.id (integer PK), as returned by
                         list_orders() as ProductionOrder.id and encoded into
                         the RFID tag by plc_client.encode_rfid().
        :param status:   New status string: "Pending", "In Progress",
                         "Completed", or "Failed".
        """
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE production_orders SET status = ? WHERE id = ?",
                (status, order_pk),
            )
            connection.commit()

        if cursor.rowcount == 0:
            self.last_error = f"Order with id={order_pk} not found."
            return False

        self.last_error = ""
        return True

    # ── process data logging ──────────────────────────────────────────────────

    def log_process_data(
        self,
        order_id: int,
        station_id: int,
        actual_start: str,
        actual_end: str,
        good_units: int,
        defect_count: int,
    ) -> None:
        """
        Record the result of a completed drilling cycle.

        :param order_id:     production_orders.id (integer PK).
        :param station_id:   stations.id of the station that ran the job.
        :param actual_start: ISO-format UTC timestamp when drilling began.
        :param actual_end:   ISO-format UTC timestamp when drilling finished.
        :param good_units:   Number of units produced without defects.
        :param defect_count: Number of defective units in this cycle.

        TODO (D1 — quality tracking): good_units and defect_count are currently
        hardcoded to 1 and 0 in the controller.  When defect entry is added to
        the GUI, pass actual counts here.
        """
        with self._connect() as connection:
            process_data_columns = self._table_columns(connection, "process_data")
            if {"timestamp", "tag_name", "value", "station"}.issubset(process_data_columns):
                connection.execute(
                    """
                    INSERT INTO process_data
                        (order_id, timestamp, tag_name, value, station,
                         actual_start, actual_end, good_units, defect_count, station_id)
                    VALUES (?, ?, '', '', '', ?, ?, ?, ?, ?)
                    """,
                    (
                        order_id,
                        datetime.now(timezone.utc).isoformat(),
                        actual_start,
                        actual_end,
                        good_units,
                        defect_count,
                        station_id,
                    ),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO process_data
                        (order_id, station_id, actual_start, actual_end, good_units, defect_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_id,
                        station_id,
                        actual_start,
                        actual_end,
                        good_units,
                        defect_count,
                    ),
                )
            connection.commit()

        self.last_error = ""

    def list_process_data(self, order_id: str | None = None) -> list[dict]:
        with self._connect() as connection:
            process_data_columns = self._table_columns(connection, "process_data")
            select_columns = [
                column
                for column in [
                    "id",
                    "order_id",
                    "station_id",
                    "actual_start",
                    "actual_end",
                    "good_units",
                    "defect_count",
                ]
                if column in process_data_columns
            ]
            sql = f"SELECT {', '.join(select_columns)} FROM process_data"
            parameters: tuple[object, ...] = ()
            if order_id is not None:
                sql += " WHERE order_id = ?"
                parameters = (order_id,)
            sql += " ORDER BY id DESC"
            rows = connection.execute(sql, parameters).fetchall()

        self.last_error = ""
        return [dict(row) for row in rows]

    # ── display helpers ───────────────────────────────────────────────────────

    def get_logged_in_user_display(self, username: str) -> str:
        normalized_username = username.strip()
        if not normalized_username:
            return "Logged in as: Guest"
        return f"Logged in as: {normalized_username}"

    # ── private ───────────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
        return {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})")}

    def _username_exists(self, username: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM users WHERE lower(username) = lower(?)",
                (username.strip(),),
            ).fetchone()
        return row is not None

    def _station_name_exists(self, name: str, exclude_station_id: int | None = None) -> bool:
        sql = "SELECT 1 FROM stations WHERE lower(name) = lower(?)"
        parameters: tuple[object, ...] = (name.strip(),)
        if exclude_station_id is not None:
            sql += " AND id != ?"
            parameters = (name.strip(), exclude_station_id)

        with self._connect() as connection:
            row = connection.execute(sql, parameters).fetchone()
        return row is not None

    @staticmethod
    def _row_to_user(row: sqlite3.Row) -> User:
        return User(id=row["id"], username=row["username"], password=row["password"])

    @staticmethod
    def _row_to_station(row: sqlite3.Row) -> Station:
        return Station(
            id=row["id"],
            name=row["name"],
            address=row["address"],
            ui_file=row["ui_file"] or "",
            enabled=bool(row["enabled"]),
        )

    @staticmethod
    def _row_to_order(row: sqlite3.Row) -> ProductionOrder:
        return ProductionOrder(
            id=row["id"],
            order_id=row["order_id"],
            recipe=row["recipe"],
            quantity=row["quantity"],
            status=row["status"],
            created_by=row["created_by"],
            created_at=row["created_at"],
            priority=row["priority"],
            rfid_tag=row["rfid_tag"],
        )
