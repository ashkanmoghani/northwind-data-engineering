from datetime import datetime
from common.consume import consume


from common.connections import get_clickhouse_connection

client = get_clickhouse_connection()


from common.connections import get_staging_connection
from common.geography import geography_key

geo_db = get_staging_connection()


def get_geography_key(data):
    return geography_key(
        client,
        geo_db,
        [data.get(k) for k in ("country", "region", "city", "postalcode", "address")],
    )


def get_current_customer(customerid):
    result = client.query(
        """
            select
                customerkey,
                geographykey,
                companyname,
                contactname,
                contacttitle,
                phone,
                fax
            from dimcustomer
            where customeralternatekey = %(customerid)s
            and iscurrent = 1
            order by 
                customerkey desc
            limit 1
        """,
        parameters={"customerid": customerid},
    )

    if result.result_rows:
        return result.result_rows[0]

    return None


def insert_customer(data):
    customerid = data.get("customerid")

    current = get_current_customer(customerid)

    if current:
        print(f"customer already exists: {customerid}")
        return

    geographykey = get_geography_key(data)

    customerkey = client.command("""
            select 
                ifNull(max(customerkey), 0) + 1
            from dimcustomer
        """)

    client.insert(
        "dimcustomer",
        [
            [
                customerkey,
                customerid,
                geographykey,
                data.get("companyname"),
                data.get("contactname"),
                data.get("contacttitle"),
                data.get("phone"),
                data.get("fax"),
                datetime.now(),
                None,
                1,
            ]
        ],
        column_names=[
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

    print(f"inserted customerid: {customerid}")


def update_customer(data):
    customerid = data.get("customerid")

    current = get_current_customer(customerid)

    if not current:
        insert_customer(data)
        return

    geographykey = get_geography_key(data)

    if tuple(current[1:]) == (
        geographykey,
        *(
            data.get(k)
            for k in ("companyname", "contactname", "contacttitle", "phone", "fax")
        ),
    ):
        return
    customerkey = current[0]

    client.command(
        """
            alter table dimcustomer
                update
                    enddate = now(),
                    iscurrent = 0
            where customerkey = %(customerkey)s
        """,
        parameters={"customerkey": customerkey},
    )

    new_customerkey = client.command("""
            select 
                ifNull(max(customerkey), 0) + 1
            from dimcustomer
        """)

    client.insert(
        "dimcustomer",
        [
            [
                new_customerkey,
                customerid,
                geographykey,
                data.get("companyname"),
                data.get("contactname"),
                data.get("contacttitle"),
                data.get("phone"),
                data.get("fax"),
                datetime.now(),
                None,
                1,
            ]
        ],
        column_names=[
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

    print(f"updated customerid with scd2: {customerid}")


def delete_customer(data):
    customerid = data.get("customerid")

    current = get_current_customer(customerid)

    if not current:
        print(f"current customer not found: {customerid}")
        return

    customerkey = current[0]

    client.command(
        """
            alter table dimcustomer
                update
                    enddate = now(),
                    iscurrent = 0
            where customerkey = %(customerkey)s
        """,
        parameters={"customerkey": customerkey},
    )

    print(f"expired customerid: {customerid}")


def handle(data):
    if not data.get("customerid"):
        raise ValueError("Missing customerid")
    functions = {
        "insert": insert_customer,
        "update": update_customer,
        "delete": delete_customer,
    }
    functions[data["action"]](data)


consume("customer_consumer", ["dimcustomer_changes"], handle)
