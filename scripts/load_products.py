from common.connections import get_source_connection, get_staging_connection

sql_conn = get_source_connection()
sql_cursor = sql_conn.cursor()

sql_cursor.execute("""
                    select
                        p.productid,
                        p.supplierid,
                        p.productname,
                        c.categoryname,
                        p.quantityperunit,
                        p.unitprice,
                        p.unitsinstock,
                        p.unitsonorder,
                        p.reorderlevel,
                        p.discontinued
                    from products p
                    left join categories c
                    on p.categoryid = c.categoryid
                    """)

rows = sql_cursor.fetchall()

print(f"extracted product rows: {len(rows)}")

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("truncate table staging_products")

for row in rows:
    pg_cursor.execute(
        """
                    insert into staging_products
                    (
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
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
        tuple(row),
    )

pg_conn.commit()

print(f"loaded product rows: {len(rows)}")

sql_cursor.close()
sql_conn.close()
pg_cursor.close()
pg_conn.close()
