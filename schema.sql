CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    address TEXT NOT NULL,
    ui_file TEXT,
    enabled INTEGER NOT NULL DEFAULT 1
);

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
);

CREATE TABLE IF NOT EXISTS process_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value TEXT NOT NULL,
    station TEXT NOT NULL
);
