from common.connections import get_dw_connection, get_clickhouse_connection

sql_conn = get_dw_connection()
sql_cursor = sql_conn.cursor()

client = get_clickhouse_connection()


def load_table(table_name, query, column_names):
    sql_cursor.execute(query)
    rows = sql_cursor.fetchall()

    data = [tuple(row) for row in rows]

    if not data:
        raise RuntimeError(f"Empty SQL dimension: {table_name}")
    if client.command(f"SELECT count() FROM {table_name}"):
        previous = client.query(
            f"SELECT {', '.join(column_names)} FROM {table_name}"
        ).result_rows

        def normalized(row):
            from datetime import datetime

            return tuple(
                v.replace(microsecond=0, tzinfo=None) if isinstance(v, datetime) else v
                for v in row
            )

        if sorted(map(normalized, previous), key=lambda r: r[0]) == sorted(
            map(normalized, data), key=lambda r: r[0]
        ):
            return
        raise RuntimeError(f"Existing dimension differs from seed: {table_name}")

    if data:
        client.insert(table_name, data, column_names=column_names)

    print(f"{table_name} rows loaded: {len(data)}")


load_table(
    "dimgeography",
    """
        select
        geographykey,
        country,
        region,
        city,
        postalcode,
        address
        from dw.dimgeography
        """,
    ["geographykey", "country", "region", "city", "postalcode", "address"],
)

load_table(
    "dimsupplier",
    """
        select
        supplierkey,
        supplierid,
        companyname,
        contactname,
        contacttitle,
        address,
        city,
        country,
        loaddate
        from dw.dimsupplier
        """,
    [
        "supplierkey",
        "supplierid",
        "companyname",
        "contactname",
        "contacttitle",
        "address",
        "city",
        "country",
        "loaddate",
    ],
)

load_table(
    "dimproducts",
    """
        select
        productkey,
        productalternatekey,
        supplierkey,
        productname,
        categoryname,
        quantityperunit,
        unitprice,
        unitsinstock,
        unitsonorder,
        reorderlevel,
        discontinued,
        startdate,
        enddate
        from dw.dimproducts
        """,
    [
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

load_table(
    "dimcustomer",
    """
        select
        customerkey,
        customeralternatekey,
        geographykey,
        companyname,
        contactname,
        contacttitle,
        phone,
        fax,
        startdate,
        enddate,
        iscurrent
        from dw.dimcustomer
        """,
    [
        "customerkey",
        "customeralternatekey",
        "geographykey",
        "companyname",
        "contactname",
        "contacttitle",
        "phone",
        "fax",
        "startdate",
        "enddate",
        "iscurrent",
    ],
)

load_table(
    "dimemployee",
    """
        select
        employeekey,
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
        reportsto,
        loaddate
        from dw.dimemployee
        """,
    [
        "employeekey",
        "employeealternatekey",
        "geographykey",
        "lastname",
        "firstname",
        "title",
        "titleofcourtesy",
        "birthdate",
        "hiredate",
        "homephone",
        "extension",
        "reportsto",
        "loaddate",
    ],
)

load_table(
    "dimshipper",
    """
        select
        shipperkey,
        shipperalternatekey,
        companyname,
        phone,
        loaddate
        from dw.dimshipper
        """,
    ["shipperkey", "shipperalternatekey", "companyname", "phone", "loaddate"],
)

load_table(
    "dimdate",
    """
        select
        datekey,
        fulldate,
        daynumber,
        dayname,
        monthnumber,
        monthname,
        quarternumber,
        yearnumber,
        weeknumber
        from dw.dimdate
        """,
    [
        "datekey",
        "fulldate",
        "daynumber",
        "dayname",
        "monthnumber",
        "monthname",
        "quarternumber",
        "yearnumber",
        "weeknumber",
    ],
)

sql_cursor.close()
sql_conn.close()

print("all clickhouse dimensions loaded")
