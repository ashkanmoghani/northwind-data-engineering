from common.connections import get_source_connection, get_staging_connection

sql_conn = get_source_connection()
sql_cursor = sql_conn.cursor()

sql_cursor.execute("""
                    select
                        country,
                        region,
                        city,
                        postalcode,
                        address
                    from(
                        select 
                            country, region, city, postalcode, address
                        from customers
                        union
                        select 
                            country, region, city, postalcode, address
                        from employees
                        union
                        select 
                            country, region, city, postalcode, address
                        from suppliers
                        union
                        select
                            shipcountry,
                            shipregion,
                            shipcity,
                            shippostalcode,
                            shipaddress
                        from orders
                        ) x 
                    """)

rows = sql_cursor.fetchall()

print(f"extracted geography rows: {len(rows)}")

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("truncate table staging_geography")

for row in rows:
    pg_cursor.execute(
        """
        insert into staging_geography
        (
        country,
        region,
        city,
        postal_code,
        address
        )
        values (%s, %s, %s, %s, %s)
        """,
        tuple(row),
    )

pg_conn.commit()

print(f"loaded geography rows: {len(rows)}")

sql_cursor.close()
sql_conn.close()
pg_cursor.close()
pg_conn.close()
