from common.connections import get_staging_connection, get_dw_connection

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
                select
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
                from staging_employees
                """)

rows = pg_cursor.fetchall()

print(f"rows read from staging: {len(rows)}")

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

inserted = 0
updated = 0

for row in rows:
    employee_id = row[0]

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
        row[11],
        row[9],
        row[8],
        row[10],
        row[7],
    )

    geography = sql_cursor.fetchone()
    geography_key = geography[0] if geography else None

    sql_cursor.execute(
        """
                    select employeekey
                    from dw.dimemployee
                    where employeealternatekey = ?
                    """,
        employee_id,
    )

    existing = sql_cursor.fetchone()

    if existing:
        sql_cursor.execute(
            """
                        update dw.dimemployee
                            set
                                geographykey = ?,
                                lastname = ?,
                                firstname = ?,
                                title = ?,
                                titleofcourtesy = ?,
                                birthdate = ?,
                                hiredate = ?,
                                homephone = ?,
                                extension = ?,
                                reportsto = ?,
                                loaddate = getdate()
                        where employeealternatekey = ?
                        """,
            geography_key,
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[12],
            row[13],
            row[14],
            employee_id,
        )

        updated += 1
    else:
        sql_cursor.execute(
            """
                        insert into dw.dimemployee
                        (
                        employeealternatekey,
                        geographykey,
                        lastname,
                        firstname,
                        title,
                        titleofcourtesy,
                        birthdate,
                        hiredate,
                        homephone,
                        extension,
                        reportsto
                        )
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
            employee_id,
            geography_key,
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[12],
            row[13],
            row[14],
        )

        inserted += 1

sql_conn.commit()

print(f"inserted rows: {inserted}")
print(f"updated rows: {updated}")

pg_cursor.close()
pg_conn.close()
sql_cursor.close()
sql_conn.close()
