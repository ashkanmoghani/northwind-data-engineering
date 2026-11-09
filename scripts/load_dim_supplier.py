from common.connections import get_staging_connection, get_dw_connection

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
                select
                    supplier_id,
                    company_name,
                    contact_name,
                    contact_title,
                    address,
                    city,
                    country
                from staging_suppliers
                order by supplier_id
                """)

rows = pg_cursor.fetchall()

print(f"rows read from staging: {len(rows)}")

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

inserted = 0
updated = 0

for row in rows:
    supplier_id = row[0]

    sql_cursor.execute(
        """
                    select supplierkey
                    from dw.dimsupplier
                    where supplierid = ?
                    """,
        supplier_id,
    )

    existing = sql_cursor.fetchone()

    if existing:
        sql_cursor.execute(
            """
                        update dw.dimsupplier
                            set
                                companyname = ?,
                                contactname = ?,
                                contacttitle = ?,
                                address = ?,
                                city = ?,
                                country = ?,
                                loaddate = getdate()
                        where supplierid = ?
                        """,
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            supplier_id,
        )

        updated += 1
    else:
        sql_cursor.execute(
            """
                        insert into dw.dimsupplier
                        (
                        supplierid,
                        companyname,
                        contactname,
                        contacttitle,
                        address,
                        city,
                        country,
                        loaddate
                        )
                        values (?, ?, ?, ?, ?, ?, ?, getdate())
                        """,
            supplier_id,
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
        )

        inserted += 1

sql_conn.commit()

print(f"inserted rows: {inserted}")
print(f"updated rows: {updated}")

pg_cursor.close()
pg_conn.close()
sql_cursor.close()
sql_conn.close()
