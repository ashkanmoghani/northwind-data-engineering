from common.connections import get_staging_connection, get_dw_connection

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
                select
                    shipper_id,
                    company_name,
                    phone
                from staging_shippers
                """)

rows = pg_cursor.fetchall()

print(f"rows read from staging: {len(rows)}")

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

inserted = 0
updated = 0

for row in rows:
    shipper_id = row[0]

    sql_cursor.execute(
        """
                    select shipperkey
                    from dw.dimshipper
                    where shipperalternatekey = ?
                    """,
        shipper_id,
    )

    existing = sql_cursor.fetchone()

    if existing:
        sql_cursor.execute(
            """
                            update dw.dimshipper
                                set
                                    companyname = ?,
                                    phone = ?,
                                    loaddate = getdate()
                            where shipperalternatekey = ?
                            """,
            row[1],
            row[2],
            shipper_id,
        )

        updated += 1
    else:
        sql_cursor.execute(
            """
                    insert into dw.dimshipper
                    (
                    shipperalternatekey,
                    companyname,
                    phone
                    )
                    values (?, ?, ?)
                    """,
            shipper_id,
            row[1],
            row[2],
        )

        inserted += 1

sql_conn.commit()

print(f"inserted rows: {inserted}")
print(f"updated rows: {updated}")

pg_cursor.close()
pg_conn.close()
sql_cursor.close()
sql_conn.close()
