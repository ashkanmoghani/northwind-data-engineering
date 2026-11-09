from common.connections import get_staging_connection, get_dw_connection

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
                    select
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
                    from staging_customers
                    """)

rows = pg_cursor.fetchall()

print(f"rows read from staging: {len(rows)}")

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

inserted = 0
changed = 0
unchanged = 0

for row in rows:
    customer_id = row[0]

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
        row[8],
        row[6],
        row[5],
        row[7],
        row[4],
    )

    geography = sql_cursor.fetchone()
    geography_key = geography[0] if geography else None

    sql_cursor.execute(
        """
                    select
                        customerkey,
                        geographykey,
                        companyname,
                        contactname,
                        contacttitle,
                        phone,
                        fax
                    from dw.dimcustomer
                    where customeralternatekey = ?
                    and iscurrent = 1
                    """,
        customer_id,
    )

    existing = sql_cursor.fetchone()

    if not existing:
        sql_cursor.execute(
            """
                        insert into dw.dimcustomer
                        (
                        customeralternatekey,
                        geographykey,
                        companyname,
                        contactname,
                        contacttitle,
                        phone,
                        fax
                        )
                        values (?, ?, ?, ?, ?, ?, ?)
                        """,
            customer_id,
            geography_key,
            row[1],
            row[2],
            row[3],
            row[9],
            row[10],
        )

        inserted += 1

    else:
        current_values = (
            existing[1],
            existing[2],
            existing[3],
            existing[4],
            existing[5],
            existing[6],
        )

        new_values = (geography_key, row[1], row[2], row[3], row[9], row[10])

        if current_values != new_values:
            sql_cursor.execute(
                """
                update dw.dimcustomer
                    set
                        enddate = getdate(),
                        iscurrent = 0
                where customerkey = ?
                """,
                existing[0],
            )

            sql_cursor.execute(
                """
                            insert into dw.dimcustomer
                            (
                            customeralternatekey,
                            geographykey,
                            companyname,
                            contactname,
                            contacttitle,
                            phone,
                            fax
                            )
                            values (?, ?, ?, ?, ?, ?, ?)
                            """,
                customer_id,
                geography_key,
                row[1],
                row[2],
                row[3],
                row[9],
                row[10],
            )

            changed += 1
        else:
            unchanged += 1

sql_conn.commit()

print(f"inserted rows: {inserted}")
print(f"changed rows: {changed}")
print(f"unchanged rows: {unchanged}")

pg_cursor.close()
pg_conn.close()
sql_cursor.close()
sql_conn.close()
