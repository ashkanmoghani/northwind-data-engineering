from common.connections import get_source_connection, get_staging_connection

sql_conn = get_source_connection()
sql_cursor = sql_conn.cursor()

sql_cursor.execute("""
                    select
                        customerid,
                        companyname,
                        contactname,
                        contacttitle,
                        address,
                        city,
                        region,
                        postalcode,
                        country,
                        phone,
                        fax
                    from customers
                    """)

rows = sql_cursor.fetchall()

print(f"extracted customer rows: {len(rows)}")

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("truncate table staging_customers")

for row in rows:
    pg_cursor.execute(
        """
                    insert into staging_customers
                    (
                    customer_id,
                    company_name,
                    contact_name,
                    contact_title,
                    address,
                    city,
                    region,
                    postal_code,
                    country,
                    phone,
                    fax
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
        tuple(row),
    )

pg_conn.commit()

print(f"loaded customer rows: {len(rows)}")

sql_cursor.close()
sql_conn.close()
pg_cursor.close()
pg_conn.close()
