from decimal import Decimal


from datetime import datetime
from common.consume import consume


from common.connections import get_clickhouse_connection, get_source_connection

client = get_clickhouse_connection()

source_conn = get_source_connection()
source_cursor = source_conn.cursor()


def get_supplier_key(supplierid):
    if supplierid is None:
        return None

    result = client.query(
        """
            select supplierkey
            from dimsupplier
            where supplierid = %(supplierid)s
            limit 1
        """,
        parameters={"supplierid": supplierid},
    )

    if result.result_rows:
        return result.result_rows[0][0]

    raise ValueError(f"Supplier {supplierid} is missing from dimsupplier")


def get_category_name(categoryid):
    if categoryid is None:
        return None

    source_cursor.execute(
        """
        select CategoryName
        from Categories
        where CategoryID = ?
        """,
        (categoryid,),
    )

    row = source_cursor.fetchone()

    if row:
        return row[0]

    return None


def get_current_product(productid):
    result = client.query(
        """
            select productkey, supplierkey, productname, categoryname, quantityperunit, unitprice, unitsinstock, unitsonorder, reorderlevel, discontinued
            from dimproducts
            where productalternatekey = %(productid)s
            and enddate is null
            order by productkey desc
            limit 1
        """,
        parameters={"productid": productid},
    )

    if result.result_rows:
        return result.result_rows[0]

    return None


def insert_product(data):
    productid = data.get("productid")

    current_productkey = get_current_product(productid)

    if current_productkey is not None:
        print(f"product already exists: {productid}")
        return

    supplierkey = get_supplier_key(data.get("supplierid"))

    categoryname = get_category_name(data.get("categoryid"))

    productkey = client.command("""
            select ifNull(max(productkey), 0) + 1
            from dimproducts
        """)

    client.insert(
        "dimproducts",
        [
            [
                productkey,
                productid,
                supplierkey,
                data.get("productname"),
                categoryname,
                data.get("quantityperunit"),
                data.get("unitprice"),
                data.get("unitsinstock"),
                data.get("unitsonorder"),
                data.get("reorderlevel"),
                data.get("discontinued"),
                datetime.now(),
                None,
            ]
        ],
        column_names=[
            "productkey",
            "productalternatekey",
            "supplierkey",
            "productname",
            "categoryname",
            "quantityperunit",
            "unitprice",
            "unitsinstock",
            "unitsonorder",
            "reorderlevel",
            "discontinued",
            "startdate",
            "enddate",
        ],
    )

    print(f"inserted productid: {productid}")


def update_product(data):
    productid = data.get("productid")

    current_productkey = get_current_product(productid)

    if current_productkey is None:
        insert_product(data)
        return

    compare = (
        get_supplier_key(data.get("supplierid")),
        data["productname"],
        get_category_name(data.get("categoryid")),
        *(
            data.get(k)
            for k in (
                "quantityperunit",
                "unitprice",
                "unitsinstock",
                "unitsonorder",
                "reorderlevel",
                "discontinued",
            )
        ),
    )
    if tuple(current_productkey[1:]) == compare:
        return

    client.command(
        """
            alter table dimproducts
            update
            enddate = now()
            where productkey = %(productkey)s
        """,
        parameters={"productkey": current_productkey[0]},
    )

    supplierkey = get_supplier_key(data.get("supplierid"))

    categoryname = get_category_name(data.get("categoryid"))

    new_productkey = client.command("""
            select ifNull(max(productkey), 0) + 1
            from dimproducts
        """)

    client.insert(
        "dimproducts",
        [
            [
                new_productkey,
                productid,
                supplierkey,
                data.get("productname"),
                categoryname,
                data.get("quantityperunit"),
                data.get("unitprice"),
                data.get("unitsinstock"),
                data.get("unitsonorder"),
                data.get("reorderlevel"),
                data.get("discontinued"),
                datetime.now(),
                None,
            ]
        ],
        column_names=[
            "productkey",
            "productalternatekey",
            "supplierkey",
            "productname",
            "categoryname",
            "quantityperunit",
            "unitprice",
            "unitsinstock",
            "unitsonorder",
            "reorderlevel",
            "discontinued",
            "startdate",
            "enddate",
        ],
    )

    print(f"updated productid with scd2: {productid}")


def delete_product(data):
    productid = data.get("productid")

    current_productkey = get_current_product(productid)

    if current_productkey is None:
        print(f"current product not found: {productid}")
        return

    client.command(
        """
            alter table dimproducts
            update
            enddate = now()
            where productkey = %(productkey)s
        """,
        parameters={"productkey": current_productkey[0]},
    )

    print(f"expired productid: {productid}")


def handle(data):
    if data.get("unitprice") is not None:
        data["unitprice"] = Decimal(str(data["unitprice"]))
    if not data.get("productid"):
        raise ValueError("Missing productid")
    functions = {
        "insert": insert_product,
        "update": update_product,
        "delete": delete_product,
    }
    functions[data["action"]](data)


consume("product_consumer", ["dimproducts_changes"], handle)
