from common.connections import get_source_connection, get_staging_connection

sql_conn = get_source_connection()
sql_cursor = sql_conn.cursor()

sql_cursor.execute("""
                    select
                        shipperid,
                        companyname,
                        phone
                    from shippers
                    """)

rows = sql_cursor.fetchall()

print(f"extracted shipper rows: {len(rows)}")

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("truncate table staging_shippers")

for row in rows:
    pg_cursor.execute(
        """
                    insert into staging_shippers
                    (
                    shipper_id,
                    company_name,
                    phone
                    )
                    values (%s, %s, %s)
                    """,
        tuple(row),
    )

pg_conn.commit()

print(f"loaded shipper rows: {len(rows)}")

sql_cursor.close()
sql_conn.close()
pg_cursor.close()
pg_conn.close()
