import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(__file__))

from model import DEFAULT_DB_PATH, MesManager


db_path = str(DEFAULT_DB_PATH)
mgr = MesManager(db_path)

conn = sqlite3.connect(db_path)
conn.execute("DELETE FROM production_orders")
conn.execute("DELETE FROM process_data")
conn.execute("DELETE FROM stations")
conn.commit()
conn.close()

mgr.add_station(
    name="Drilling",
    address="172.21.3.1",
    ui_file="",
    enabled=True,
)

mgr.add_order(
    order_id="1",
    recipe="Left Holes",
    quantity=1,
    created_by="demo",
    priority=1,
)
mgr.add_order(
    order_id="2",
    recipe="All Holes",
    quantity=1,
    created_by="demo",
    priority=2,
)
mgr.add_order(
    order_id="3",
    recipe="Right Holes",
    quantity=1,
    created_by="demo",
    priority=3,
)

print("Demo DB ready.")
print()
print("DEMO STEPS:")
print("  1. python backend/demo_reset.py")
print("  2. python backend/mes_app.py")
print("  3. log in, go to New Order, and submit a drilling order")
print("  4. if the PLC is ready, the order will move to In Progress automatically")
print("  5. use PLC Diagnostics only if you want to observe or debug node activity")
print("  6. watch appRun/taskCode/writeData in the node monitor")
print("  7. when appDone=True arrives, the order should move to Completed")
print("  8. check the Order History tab for RFID/result/last-update traceability")
