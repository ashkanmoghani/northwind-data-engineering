import json
import time
from decimal import Decimal
from confluent_kafka import Producer
from common.config import kafka_bootstrap_servers
from common.connections import get_source_connection, get_staging_connection
from common.telemetry import (
    build_trace_id,
    register_raw_event,
    mark_kafka_published,
    mark_kafka_failed,
)

SOURCES = {
    "customer_cdc": (
        "Customers",
        "dimcustomer_changes",
        "CustomerID CompanyName ContactName ContactTitle Address City Region PostalCode Country Phone Fax",
    ),
    "product_cdc": (
        "Products",
        "dimproducts_changes",
        "ProductID SupplierID CategoryID ProductName QuantityPerUnit UnitPrice UnitsInStock UnitsOnOrder ReorderLevel Discontinued",
    ),
    "orders_cdc": ("Orders", "orders_changes", "OrderID"),
    "orderdetails_cdc": ("Order Details", "orderdetails_changes", "OrderID ProductID"),
}


def readable(value):
    if isinstance(value, Decimal):
        return str(value)
    return value.rstrip() if isinstance(value, str) else value


def confirm(producer, topic, key, event):
    delivered = []

    def acknowledge(error, msg):
        if error is None:
            delivered.append((msg.topic(), msg.partition(), msg.offset()))

    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(event, ensure_ascii=False).encode(),
        on_delivery=acknowledge,
    )
    if producer.flush(30) or len(delivered) != 1:
        raise RuntimeError("Kafka delivery unconfirmed; checkpoint retained")
    return delivered[0]


def run(name):
    table, topic, columns = SOURCES[name]
    fields = columns.split()
    capture = "dbo_" + table
    sql = get_source_connection()
    pg = get_staging_connection()
    reader = sql.cursor()
    state = pg.cursor()
    state.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (name,))
    if not state.fetchone()[0]:
        raise RuntimeError("Another reader owns " + name)
    state.execute("SELECT last_lsn FROM cdc_offsets WHERE pipeline_name=%s", (name,))
    row = state.fetchone()
    if not row:
        raise RuntimeError("Initial CDC setup is required")
    last = bytes.fromhex(row[0][2:])
    pg.commit()
    producer = Producer(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 25000,
        }
    )
    try:
        while True:
            reader.execute(
                "SELECT sys.fn_cdc_get_min_lsn(?),sys.fn_cdc_get_max_lsn()", capture
            )
            low, high = map(bytes, reader.fetchone())
            if not any(low) or not low <= last <= high:
                raise RuntimeError("CDC checkpoint outside retention for " + table)
            if high != last:
                reader.execute(
                    f"SELECT __$start_lsn,__$seqval,__$operation,__$command_id,{','.join(fields)} FROM cdc.[{capture}_CT] WHERE __$start_lsn>? AND __$start_lsn<=? ORDER BY __$start_lsn,__$command_id,__$seqval,__$operation",
                    last,
                    high,
                )
                for lsn, seq, op, command, *values in reader.fetchall():
                    if op == 3:
                        continue
                    payload = {k.lower(): readable(v) for k, v in zip(fields, values)}
                    key = str(values[0]).strip()
                    identity = key + (
                        ":" + str(values[1]) if name == "orderdetails_cdc" else ""
                    )
                    trace = build_trace_id(f"{name}/{command}/{identity}", lsn, seq, op)
                    action = {1: "delete", 2: "insert", 4: "update"}[op]
                    payload.update(trace_id=trace, action=action)
                    encoded = "0x" + bytes(lsn).hex()
                    status = register_raw_event(
                        state,
                        pg,
                        name,
                        trace,
                        table,
                        action,
                        identity,
                        payload.get("orderid"),
                        payload,
                        encoded,
                    )
                    if status in ("published", "processing", "processed"):
                        continue
                    try:
                        delivery = confirm(producer, topic, key, payload)
                    except Exception as exc:
                        exc.classic_trace = trace
                        mark_kafka_failed(state, pg, name, trace, str(exc))
                        raise
                    mark_kafka_published(state, pg, name, trace, *delivery)
                state.execute(
                    "UPDATE cdc_offsets SET last_lsn=%s,updated_at=now() WHERE pipeline_name=%s",
                    ("0x" + high.hex(), name),
                )
                pg.commit()
                last = high
            time.sleep(2)
    finally:
        pg.close()
        sql.close()
