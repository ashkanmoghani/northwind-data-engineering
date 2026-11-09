import json
from decimal import Decimal, ROUND_HALF_EVEN
from common.connections import get_source_connection, get_clickhouse_connection


def money(value):
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)


def totals(rows):
    gross = Decimal(0)
    rebates = Decimal(0)
    orders = set()
    count = 0
    for order, price, quantity, discount in rows:
        line = money(Decimal(str(price)) * quantity)
        rebate = money(line * Decimal(str(round(discount, 4))))
        orders.add(order)
        gross += line
        rebates += rebate
        count += 1
    return len(orders), count, gross, rebates, gross - rebates


def main():
    src = get_source_connection()
    ch = get_clickhouse_connection()
    try:
        cur = src.cursor()
        cur.execute(
            "SELECT OrderID,UnitPrice,Quantity,Discount FROM dbo.[Order Details]"
        )
        source = totals(cur)
        wh = ch.query(
            "SELECT uniqExact(orderid),count(),sum(grossamount),sum(discountamount),sum(netamount),count()-uniqExact(tuple(orderid,productkey)) FROM factorders"
        ).result_rows[0]
        dimensions = {}
        for table, key, field, condition in [
            ("Customers", "dimcustomer", "customeralternatekey", "iscurrent=1"),
            ("Products", "dimproducts", "productalternatekey", "enddate IS NULL"),
        ]:
            cur.execute("SELECT count(*) FROM dbo." + table)
            source_count = cur.fetchone()[0]
            total, unique = ch.query(
                f"SELECT count(),uniqExact({field}) FROM {key} WHERE {condition}"
            ).result_rows[0]
            dimensions[table] = {
                "source": source_count,
                "current": total,
                "unique": unique,
            }
        passed = (
            source[1] > 0
            and tuple(wh[:5]) == source
            and wh[5] == 0
            and all(
                v["source"] == v["current"] == v["unique"] for v in dimensions.values()
            )
        )
        print(
            json.dumps(
                {
                    "passed": passed,
                    "orders": source[0],
                    "source_lines": source[1],
                    "warehouse_lines": wh[1],
                    "source_net": str(source[4]),
                    "warehouse_net": str(wh[4]),
                    "duplicate_facts": wh[5],
                    "dimensions": dimensions,
                },
                ensure_ascii=False,
            )
        )
        return 0 if passed else 1
    finally:
        src.close()
        ch.close()


if __name__ == "__main__":
    raise SystemExit(main())
