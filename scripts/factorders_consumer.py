from decimal import Decimal, ROUND_HALF_EVEN
from common.consume import consume


from common.connections import (
    get_clickhouse_connection,
    get_source_connection,
    get_staging_connection,
)

client = get_clickhouse_connection()

source_conn = get_source_connection()
source_cursor = source_conn.cursor()

pg_conn = get_staging_connection()
pg_cursor = pg_conn.cursor()

pipeline_name = "factorders_consumer"


def get_customer_key(customerid):
    if customerid is None:
        return None

    customerid = customerid.strip()

    result = client.query(
        """
            select customerkey
            from northwind_dw.dimcustomer
            where customeralternatekey = %(customerid)s
            and iscurrent = 1
            order by customerkey desc
            limit 1
        """,
        parameters={"customerid": customerid},
    )

    if result.result_rows:
        return result.result_rows[0][0]

    return None


def get_employee_key(employeeid):
    if employeeid is None:
        return None

    result = client.query(
        """
            select employeekey
            from dimemployee
            where employeealternatekey = %(employeeid)s
            limit 1
        """,
        parameters={"employeeid": employeeid},
    )

    if result.result_rows:
        return result.result_rows[0][0]

    return None


def get_product_key(productid):
    result = client.query(
        """
            select productkey
            from dimproducts
            where productalternatekey = %(productid)s
            and enddate is null
            order by productkey desc
            limit 1
        """,
        parameters={"productid": productid},
    )

    if result.result_rows:
        return result.result_rows[0][0]

    return None


def get_shipper_key(shipvia):
    if shipvia is None:
        return None

    result = client.query(
        """
            select shipperkey
            from dimshipper
            where shipperalternatekey = %(shipvia)s
            limit 1
        """,
        parameters={"shipvia": shipvia},
    )

    if result.result_rows:
        return result.result_rows[0][0]

    return None


def get_date_key(value):
    if value is None:
        return None

    value = value.date()

    result = client.query(
        """
            select datekey
            from dimdate
            where fulldate = %(fulldate)s
            limit 1
        """,
        parameters={"fulldate": value},
    )

    if result.result_rows:
        return result.result_rows[0][0]

    return None


from common.geography import geography_key


def get_geography_key(order):
    return geography_key(
        client,
        pg_conn,
        [
            order.get("ship" + k)
            for k in ("country", "region", "city", "postalcode", "address")
        ],
    )


def delete_fact_order(orderid):
    client.command(
        """
            alter table factorders
            delete
            where orderid = %(orderid)s
        """,
        parameters={"orderid": orderid},
        settings={"mutations_sync": 1},
    )


def get_order(orderid):
    source_cursor.execute(
        """
        select
        OrderID,
        CustomerID,
        EmployeeID,
        OrderDate,
        RequiredDate,
        ShippedDate,
        ShipVia,
        ShipName,
        ShipAddress,
        ShipCity,
        ShipRegion,
        ShipPostalCode,
        ShipCountry
        from Orders
        where OrderID = ?
        """,
        (orderid,),
    )

    row = source_cursor.fetchone()

    if not row:
        return None

    return {
        "orderid": row[0],
        "customerid": row[1].strip() if row[1] else None,
        "employeeid": row[2],
        "orderdate": row[3],
        "requireddate": row[4],
        "shippeddate": row[5],
        "shipvia": row[6],
        "shipname": row[7],
        "shipaddress": row[8],
        "shipcity": row[9],
        "shipregion": row[10],
        "shippostalcode": row[11],
        "shipcountry": row[12],
    }


def get_order_details(orderid):
    source_cursor.execute(
        """
        select
        ProductID,
        UnitPrice,
        Quantity,
        Discount
        from [Order Details]
        where OrderID = ?
        order by ProductID
        """,
        (orderid,),
    )

    return source_cursor.fetchall()


def rebuild_order(orderid):
    order = get_order(orderid)

    if order is None:
        delete_fact_order(orderid)

        print(f"deleted fact orderid: {orderid}")
        return

    details = get_order_details(orderid)

    if not details:
        delete_fact_order(orderid)

        print(f"no order details for orderid: {orderid}")
        return

    customerkey = get_customer_key(order["customerid"])

    employeekey = get_employee_key(order["employeeid"])

    orderdatekey = get_date_key(order["orderdate"])

    requireddatekey = get_date_key(order["requireddate"])

    shippeddatekey = get_date_key(order["shippeddate"])

    shipperkey = get_shipper_key(order["shipvia"])

    geographykey = get_geography_key(order)

    if customerkey is None:
        raise ValueError(f"customer lookup failed for orderid: {orderid}")

    if employeekey is None:
        raise ValueError(f"employee lookup failed for orderid: {orderid}")

    if orderdatekey is None:
        raise ValueError(f"order date lookup failed for orderid: {orderid}")

    if requireddatekey is None:
        raise ValueError(f"required date lookup failed for orderid: {orderid}")

    if order["shippeddate"] is not None and shippeddatekey is None:
        raise ValueError(f"shipped date lookup failed for orderid: {orderid}")

    if order["shipvia"] is not None and shipperkey is None:
        raise ValueError(f"shipper lookup failed for orderid: {orderid}")

    if geographykey is None:
        raise ValueError(f"geography lookup failed for orderid: {orderid}")

    rows = []

    for detail in details:
        productid = detail[0]

        productkey = get_product_key(productid)

        if productkey is None:
            raise ValueError(f"product lookup failed for productid: {productid}")

        unitprice = Decimal(str(detail[1])).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )

        quantity = detail[2]

        discount = Decimal(str(round(detail[3], 4))).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_EVEN
        )

        grossamount = (unitprice * Decimal(quantity)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )

        discountamount = (grossamount * discount).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )

        netamount = (grossamount - discountamount).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )
        rows.append(
            [
                orderid,
                productkey,
                customerkey,
                employeekey,
                requireddatekey,
                shippeddatekey,
                orderdatekey,
                shipperkey,
                geographykey,
                order["shipname"],
                unitprice,
                quantity,
                discount,
                grossamount,
                discountamount,
                netamount,
                order["orderdate"].date(),
                order["requireddate"].date(),
                (order["shippeddate"].date() if order["shippeddate"] else None),
            ]
        )

    if not rows:
        raise ValueError(f"no fact rows prepared for orderid: {orderid}")

    delete_fact_order(orderid)

    client.insert(
        "factorders",
        rows,
        column_names=[
            "orderid",
            "productkey",
            "customerkey",
            "employeekey",
            "requireddatekey",
            "shippeddatekey",
            "orderdatekey",
            "shipperkey",
            "geographykey",
            "shipname",
            "unitprice",
            "quantity",
            "discount",
            "grossamount",
            "discountamount",
            "netamount",
            "orderdate",
            "requireddate",
            "shippeddate",
        ],
    )

    print(f"rebuilt orderid: {orderid} with {len(rows)} fact row(s)")


def handle(data):
    orderid = data.get("orderid")
    if type(orderid) is not int or orderid <= 0:
        raise ValueError("Invalid orderid")
    rebuild_order(orderid)


consume("factorders_consumer", ["orders_changes", "orderdetails_changes"], handle)
