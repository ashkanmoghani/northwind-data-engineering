from datetime import date, timedelta
from common.connections import get_dw_connection

conn = get_dw_connection()
cursor = conn.cursor()

start_date = date(1990, 1, 1)
end_date = date(2035, 12, 31)

current_date = start_date
inserted = 0

cursor.execute("SELECT datekey FROM dw.dimdate")
existing = {row[0] for row in cursor.fetchall()}
rows = []
while current_date <= end_date:
    key = int(current_date.strftime("%Y%m%d"))
    if key not in existing:
        rows.append(
            (
                key,
                current_date,
                current_date.day,
                current_date.strftime("%A"),
                current_date.month,
                current_date.strftime("%B"),
                (current_date.month - 1) // 3 + 1,
                current_date.year,
                current_date.isocalendar().week,
            )
        )
    current_date += timedelta(days=1)
if rows:
    cursor.fast_executemany = True
    cursor.executemany(
        "INSERT INTO dw.dimdate (datekey,fulldate,daynumber,dayname,monthnumber,monthname,quarternumber,yearnumber,weeknumber) VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
conn.commit()
print("Date rows inserted:", len(rows))
cursor.close()
conn.close()
