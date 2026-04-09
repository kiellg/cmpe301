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

print("Demo DB ready.")
print()
print("DEMO STEPS:")
print("  1. python backend/demo_reset.py")
print("  2. python backend/mes_app.py")
print("  3. log in, go to New Order, and submit the demo drilling order")
print("  4. there are no preloaded orders, so the first order you create is the one that runs")
print("  5. if the PLC is ready, the order will move to In Progress automatically")
print("  6. use PLC Diagnostics only if you want to observe or debug node activity")
print("  7. watch appRun/taskCode/writeData in the node monitor")
print("  8. when appDone=True arrives, the order should move to Completed")
print("  9. check the Order History tab for RFID/result/last-update traceability")
