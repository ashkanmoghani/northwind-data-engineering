import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    lit,
    col,
    bround,
    when,
    date_format,
    sum as spark_sum,
)

import clickhouse_connect
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--publish", action="store_true")
args = parser.parse_args()


spark = SparkSession.builder.appName(
    "northwind_factorders_dimension_lookup"
).getOrCreate()


postgres_url = "jdbc:postgresql://northwind_postgres:5432/" "Staging_Northwind"

postgres_properties = {
    "user": "postgres",
    "password": os.environ["postgres_password"],
    "driver": "org.postgresql.Driver",
}


fact = spark.read.jdbc(
    url=postgres_url, table="public.staging_factorders", properties=postgres_properties
)


fact = (
    fact.withColumn("unit_price", bround(col("unit_price"), 2))
    .withColumn("discount", bround(col("discount"), 4))
    .cache()
)
fact_count = fact.count()
print(f"fact rows: {fact_count}")
if fact_count == 0:
    raise ValueError("staging_factorders is empty; existing DW will be preserved")


client = clickhouse_connect.get_client(
    host="northwind_clickhouse",
    port=8123,
    username="clickhouse",
    password=os.environ["clickhouse_password"],
    database="northwind_dw",
)


customer_rows = client.query("""
    select
        customeralternatekey,
        customerkey
    from dimcustomer
    where iscurrent = 1
""").result_rows

customer_df = spark.createDataFrame(customer_rows, ["customer_id_dw", "customerkey"])


product_rows = client.query("""
    select
        productalternatekey,
        productkey
    from dimproducts
    where enddate is null
""").result_rows

product_df = spark.createDataFrame(product_rows, ["product_id_dw", "productkey"])


employee_rows = client.query("""
    select
        employeealternatekey,
        employeekey
    from dimemployee
""").result_rows

employee_df = spark.createDataFrame(employee_rows, ["employee_id_dw", "employeekey"])


shipper_rows = client.query("""
    select
        shipperalternatekey,
        shipperkey
    from dimshipper
""").result_rows

shipper_df = spark.createDataFrame(shipper_rows, ["shipper_id_dw", "shipperkey"])


geography_rows = client.query("""
    select
        geographykey,
        country,
        region,
        city,
        postalcode,
        address
    from dimgeography
""").result_rows

geography_df = spark.createDataFrame(
    geography_rows,
    [
        "geographykey",
        "country_dw",
        "region_dw",
        "city_dw",
        "postal_code_dw",
        "address_dw",
    ],
)


result = (
    fact.join(
        broadcast(customer_df), fact.customer_id == customer_df.customer_id_dw, "left"
    )
    .join(broadcast(product_df), fact.product_id == product_df.product_id_dw, "left")
    .join(
        broadcast(employee_df), fact.employee_id == employee_df.employee_id_dw, "left"
    )
    .join(broadcast(shipper_df), fact.ship_via == shipper_df.shipper_id_dw, "left")
    .join(
        broadcast(geography_df),
        (
            (
                coalesce(fact.ship_country, lit(""))
                == coalesce(geography_df.country_dw, lit(""))
            )
            & (
                coalesce(fact.ship_region, lit(""))
                == coalesce(geography_df.region_dw, lit(""))
            )
            & (
                coalesce(fact.ship_city, lit(""))
                == coalesce(geography_df.city_dw, lit(""))
            )
            & (
                coalesce(fact.ship_postal_code, lit(""))
                == coalesce(geography_df.postal_code_dw, lit(""))
            )
            & (
                coalesce(fact.ship_address, lit(""))
                == coalesce(geography_df.address_dw, lit(""))
            )
        ),
        "left",
    )
)


result = (
    result.withColumn("grossamount", bround(col("unit_price") * col("quantity"), 2))
    .withColumn("discountamount", bround(col("grossamount") * col("discount"), 2))
    .withColumn("netamount", bround(col("grossamount") - col("discountamount"), 2))
)

result = (
    result.withColumn(
        "orderdatekey", date_format(col("order_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "requireddatekey", date_format(col("required_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "shippeddatekey",
        when(
            col("shipped_date").isNotNull(),
            date_format(col("shipped_date"), "yyyyMMdd").cast("int"),
        ),
    )
)

result = result.cache()
result_count = result.count()
print(f"result rows: {result_count}")
if result_count != fact_count:
    raise ValueError("Dimension joins changed the number of fact rows")

for key in [
    "productkey",
    "customerkey",
    "employeekey",
    "geographykey",
    "orderdatekey",
    "requireddatekey",
]:
    missing = result.filter(col(key).isNull()).count()
    print(f"missing {key}: {missing}")
    if missing:
        raise ValueError(f"Missing required key: {key}")

if result.filter(col("ship_via").isNotNull() & col("shipperkey").isNull()).count():
    raise ValueError("Missing shipper lookup")

final_df = result.select(
    col("order_id").alias("orderid"),
    "productkey",
    "customerkey",
    "employeekey",
    "requireddatekey",
    "shippeddatekey",
    "orderdatekey",
    "shipperkey",
    "geographykey",
    col("ship_name").alias("shipname"),
    col("unit_price").alias("unitprice"),
    "quantity",
    "discount",
    "grossamount",
    "discountamount",
    "netamount",
    col("order_date").cast("date").alias("orderdate"),
    col("required_date").cast("date").alias("requireddate"),
    col("shipped_date").cast("date").alias("shippeddate"),
)

final_df = final_df.cache()
if final_df.select("orderid", "productkey").distinct().count() != fact_count:
    raise ValueError("Duplicate (orderid, productkey) in Spark output")

date_rows = client.query("SELECT datekey, fulldate FROM dimdate").result_rows
date_df = spark.createDataFrame(date_rows, "datekey long, fulldate date")
for key, date_column in [
    ("orderdatekey", "orderdate"),
    ("requireddatekey", "requireddate"),
    ("shippeddatekey", "shippeddate"),
]:
    missing_dates = (
        final_df.filter(col(date_column).isNotNull())
        .join(
            date_df,
            (col(key) == date_df.datekey) & (col(date_column) == date_df.fulldate),
            "left_anti",
        )
        .limit(1)
        .count()
    )
    if missing_dates:
        raise ValueError(f"Date lookup failed: {key}")

expected_amounts = final_df.agg(
    spark_sum("grossamount"), spark_sum("discountamount"), spark_sum("netamount")
).first()

client.command("CREATE TABLE IF NOT EXISTS factorders_spark_build AS factorders")
client.command("TRUNCATE TABLE factorders_spark_build")

final_df.write.format("jdbc").option(
    "url", "jdbc:clickhouse://clickhouse:8123/northwind_dw"
).option("dbtable", "factorders_spark_build").option(
    "driver", "com.clickhouse.jdbc.ClickHouseDriver"
).option(
    "user", "clickhouse"
).option(
    "password", os.environ["clickhouse_password"]
).mode(
    "append"
).save()

stored = client.query("""
    SELECT
        count(),
        uniqExact(tuple(orderid, productkey)),
        sum(grossamount),
        sum(discountamount),
        sum(netamount)
    FROM factorders_spark_build
""").result_rows[0]

if stored[0] != fact_count or stored[1] != fact_count:
    raise ValueError("Build table row count or uniqueness check failed")
if tuple(stored[2:]) != tuple(expected_amounts):
    raise ValueError("Build table amounts do not match Spark output")

print(
    f"Validated build: rows={stored[0]}, gross={stored[2]}, discount={stored[3]}, net={stored[4]}"
)

if args.publish:
    from common.stop import require_active

    require_active("northwind_etl", os.environ["CLASSIC_RUN_ID"])
    client.command("EXCHANGE TABLES factorders AND factorders_spark_build")
    print("Published factorders. Previous version is in factorders_spark_build.")
else:
    print("Build only; factorders was not replaced. Airflow uses --publish.")

client.close()
final_df.unpersist()
result.unpersist()
fact.unpersist()
spark.stop()
