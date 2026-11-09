from common.connections import get_staging_connection, get_dw_connection

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
                select
                    product_id,
                    supplier_id,
                    product_name,
                    category_name,
                    quantity_per_unit,
                    unit_price,
                    units_in_stock,
                    units_on_order,
                    reorder_level,
                    discontinued
                from staging_products
                """)

rows = pg_cursor.fetchall()

print(f"rows read from staging: {len(rows)}")

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

inserted = 0
updated = 0

for row in rows:
    product_id = row[0]
    supplier_id = row[1]

    sql_cursor.execute(
        """
                select supplierkey
                from dw.dimsupplier
                where supplierid = ?
                """,
        supplier_id,
    )

    supplier = sql_cursor.fetchone()
    supplier_key = supplier[0] if supplier else None

    sql_cursor.execute(
        """
                    select productkey
                    from dw.dimproducts
                    where productalternatekey = ?
                    """,
        product_id,
    )

    existing = sql_cursor.fetchone()

    if existing:
        sql_cursor.execute(
            """
            update dw.dimproducts
                set
                    supplierkey = ?,
                    productname = ?,
                    categoryname = ?,
                    quantityperunit = ?,
                    unitprice = ?,
                    unitsinstock = ?,
                    unitsonorder = ?,
                    reorderlevel = ?,
                    discontinued = ?
            where productalternatekey = ?
            """,
            supplier_key,
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            product_id,
        )

        updated += 1
    else:
        sql_cursor.execute(
            """
            insert into dw.dimproducts
            (
            productalternatekey,
            supplierkey,
            productname,
            categoryname,
            quantityperunit,
            unitprice,
            unitsinstock,
            unitsonorder,
            reorderlevel,
            discontinued
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            product_id,
            supplier_key,
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
        )

        inserted += 1

sql_conn.commit()

print(f"inserted rows: {inserted}")
print(f"updated rows: {updated}")

pg_cursor.close()
pg_conn.close()
sql_cursor.close()
sql_conn.close()
