import json
from confluent_kafka import Consumer
from common.config import kafka_bootstrap_servers
from common.connections import get_staging_connection
from common.telemetry import begin_processing, mark_processed, mark_processing_failed


def consume(name, topics, handler):
    group = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    group.subscribe(topics)
    pg = get_staging_connection()
    try:
        with pg.cursor() as cur:
            while True:
                msg = group.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(str(msg.error()))
                event = json.loads(msg.value())
                trace = event["trace_id"]
                order = event.get("orderid")
                if begin_processing(
                    cur,
                    pg,
                    name,
                    trace,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    order,
                ):
                    try:
                        handler(event)
                        mark_processed(cur, pg, name, trace, order)
                    except Exception as exc:
                        exc.classic_trace = trace
                        pg.rollback()
                        mark_processing_failed(cur, pg, name, trace, order, str(exc))
                        raise
                group.commit(message=msg, asynchronous=False)
    finally:
        group.close()
        pg.close()
