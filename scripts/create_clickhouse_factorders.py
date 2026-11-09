from common.connections import get_clickhouse_connection

client = get_clickhouse_connection()

client.command("""
            create table if not exists factorders
            (
            orderid Int32,
            productkey Int32,
            customerkey Int32,
            employeekey Int32,
            requireddatekey Int32,
            shippeddatekey Nullable(Int32),
            orderdatekey Int32,
            shipperkey Nullable(Int32),
            geographykey Int32,
            shipname Nullable(String),
            unitprice Decimal(18, 2),
            quantity Int16,
            discount Decimal(10, 4),
            grossamount Decimal(18, 2),
            discountamount Decimal(18, 2),
            netamount Decimal(18, 2),
            orderdate Date,
            requireddate Date,
            shippeddate Nullable(Date),
            loaddate DateTime default now()
            )
            engine = MergeTree
            order by (orderid, productkey)
            """)

print("clickhouse factorders table created")
