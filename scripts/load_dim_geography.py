from common.connections import get_staging_connection, get_dw_connection

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
                select
                    country,
                    region,
                    city,
                    postal_code,
                    address
                from staging_geography
                """)

rows = pg_cursor.fetchall()

print(f"rows read from staging: {len(rows)}")

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

inserted = 0

for row in rows:
    sql_cursor.execute(
        """
                    select geographykey
                    from dw.dimgeography
                    where isnull(country, '')   = isnull(?, '')
                    and isnull(region, '')      = isnull(?, '')
                    and isnull(city, '')        = isnull(?, '')
                    and isnull(postalcode, '')  = isnull(?, '')
                    and isnull(address, '')     = isnull(?, '')
                    """,
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
    )

    existing = sql_cursor.fetchone()

    if not existing:
        sql_cursor.execute(
            """
                        insert into dw.dimgeography
                        (
                        country,
                        region,
                        city,
                        postalcode,
                        address
                        )
                        values (?, ?, ?, ?, ?)
                        """,
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
        )

        inserted += 1

sql_conn.commit()

print(f"inserted rows: {inserted}")

pg_cursor.close()
pg_conn.close()
sql_cursor.close()
sql_conn.close()
