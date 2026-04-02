import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(__file__))

from model import MesManager


db_path = os.path.join(os.path.dirname(__file__), "mes.db")
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
print("  3. log in, go to Orders tab - 3 Pending orders visible")
print("  4. go to PLC Diagnostics tab")
print("  5. make sure carrier is physically at the stopper")
print("  6. click 'Write Tag' - enter order ID 1, recipe Left Holes, qty 1")
print("  7. click Write - watch log: 'Tag written: order #1'")
print("  8. click 'Read RFID Tag' - triggers fresh PLC read")
print("  9. watch node monitor: readPresence=True, readDone=True")
print(" 10. machine state: 'Carrier arrived: order #1 - Left Holes'")
print(" 11. awaitApp fires - watch appRun=True, taskCode=1 in node monitor")
print(" 12. watch the drill move physically")
print(" 13. appDone=True - order #1 -> Completed in Orders tab")
print(" 14. repeat for orders #2 and #3")
