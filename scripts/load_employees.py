from common.connections import get_source_connection, get_staging_connection

sql_conn = get_source_connection()
sql_cursor = sql_conn.cursor()

sql_cursor.execute("""
                    select
                        employeeid,
                        lastname,
                        firstname,
                        title,
                        titleofcourtesy,
                        birthdate,
                        hiredate,
                        address,
                        city,
                        region,
                        postalcode,
                        country,
                        homephone,
                        extension,
                        reportsto
                    from employees
                    """)

rows = sql_cursor.fetchall()

print(f"extracted employee rows: {len(rows)}")

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("truncate table staging_employees")

for row in rows:
    pg_cursor.execute(
        """
                    insert into staging_employees
                    (
                    employee_id,
                    last_name,
                    first_name,
                    title,
                    title_of_courtesy,
                    birth_date,
                    hire_date,
                    address,
                    city,
                    region,
                    postal_code,
                    country,
                    home_phone,
                    extension,
                    reports_to
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
        tuple(row),
    )

pg_conn.commit()

print(f"loaded employee rows: {len(rows)}")

sql_cursor.close()
sql_conn.close()
pg_cursor.close()
pg_conn.close()
