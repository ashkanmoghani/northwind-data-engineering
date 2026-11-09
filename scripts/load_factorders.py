from common.connections import get_source_connection, get_staging_connection

sql_conn = get_source_connection()
sql_cursor = sql_conn.cursor()

sql_cursor.execute("""
        select
        o.orderid,
        od.productid,
        o.customerid,
        o.employeeid,
        o.orderdate,
        o.requireddate,
        o.shippeddate,
        o.shipvia,
        o.shipname,
        o.shipaddress,
        o.shipcity,
        o.shipregion,
        o.shippostalcode,
        o.shipcountry,
        od.unitprice,
        od.quantity,
        od.discount
        from orders o
        inner join [order details] od
        on o.orderid = od.orderid
        """)

rows = sql_cursor.fetchall()

print(f"extracted factorders rows: {len(rows)}")

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pg_cursor.execute("truncate table staging_factorders")

for row in rows:
    pg_cursor.execute(
        """
        insert into staging_factorders
        (
        order_id,
        product_id,
        customer_id,
        employee_id,
        order_date,
        required_date,
        shipped_date,
        ship_via,
        ship_name,
        ship_address,
        ship_city,
        ship_region,
        ship_postal_code,
        ship_country,
        unit_price,
        quantity,
        discount
        )
        values
        (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        tuple(row),
    )

pg_conn.commit()

print(f"loaded factorders rows: {len(rows)}")

sql_cursor.close()
sql_conn.close()
pg_cursor.close()
pg_conn.close()
