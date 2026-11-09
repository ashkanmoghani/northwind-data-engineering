import hashlib
import json


def binary_to_string(value):
    return "0x" + bytes(value).hex()


def build_trace_id(source, start_lsn, seqval, operation):
    raw = "|".join(
        [source, binary_to_string(start_lsn), binary_to_string(seqval), str(operation)]
    )

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    return f"{source}-{digest}"


def register_raw_event(
    cursor,
    conn,
    pipeline_name,
    trace_id,
    source_table,
    action,
    record_key,
    orderid,
    payload,
    source_lsn,
):
    cursor.execute(
        """
        insert into ops.raw_events
        (
            trace_id,
            source_table,
            action,
            record_key,
            orderid,
            payload,
            source_lsn,
            status
        )
        values
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s::jsonb,
            %s,
            'received'
        )
        on conflict (trace_id)
        do nothing
        """,
        (
            trace_id,
            source_table,
            action,
            record_key,
            orderid,
            json.dumps(payload),
            source_lsn,
        ),
    )

    inserted = cursor.rowcount == 1

    if inserted:
        cursor.execute(
            """
            insert into ops.pipeline_events
            (
                trace_id,
                pipeline_name,
                stage,
                status,
                message
            )
            values
            (
                %s,
                %s,
                'CDC_CAPTURE',
                'success',
                %s
            )
            """,
            (trace_id, pipeline_name, f"{action} captured for {record_key}"),
        )

    cursor.execute(
        """
        select status
        from ops.raw_events
        where trace_id = %s
        """,
        (trace_id,),
    )

    status = cursor.fetchone()[0]

    conn.commit()

    return status


def mark_kafka_published(
    cursor, conn, pipeline_name, trace_id, topic, partition, offset
):
    cursor.execute(
        """
        update ops.raw_events
        set
            status = CASE WHEN status IN ('processed','processing','failed') AND kafka_offset IS NOT NULL THEN status WHEN status IN ('processed','processing') THEN status ELSE 'published' END,
            kafka_topic = %s,
            kafka_partition = %s,
            kafka_offset = %s,
            error_message = CASE WHEN status='failed' THEN error_message ELSE NULL END
        where trace_id = %s
        """,
        (topic, partition, offset, trace_id),
    )

    cursor.execute(
        """
        insert into ops.pipeline_events
        (
            trace_id,
            pipeline_name,
            stage,
            status,
            message,
            metadata
        )
        values
        (
            %s,
            %s,
            'KAFKA_PUBLISH',
            'success',
            'Kafka message published',
            %s::jsonb
        )
        """,
        (
            trace_id,
            pipeline_name,
            json.dumps({"topic": topic, "partition": partition, "offset": offset}),
        ),
    )

    conn.commit()


def mark_kafka_failed(cursor, conn, pipeline_name, trace_id, error_message):
    cursor.execute(
        """
        update ops.raw_events
        set
            status = 'failed',
            retry_count = retry_count + 1,
            error_message = %s
        where trace_id = %s and status not in ('processed','processing')
        """,
        (error_message, trace_id),
    )

    cursor.execute(
        """
        insert into ops.pipeline_events
        (
            trace_id,
            pipeline_name,
            stage,
            status,
            message
        )
        values
        (
            %s,
            %s,
            'KAFKA_PUBLISH',
            'failed',
            %s
        )
        """,
        (trace_id, pipeline_name, error_message),
    )

    conn.commit()


def begin_processing(
    cursor, conn, pipeline_name, trace_id, topic, partition, offset, orderid
):
    cursor.execute(
        """
        select status
        from ops.raw_events
        where trace_id = %s
        """,
        (trace_id,),
    )

    row = cursor.fetchone()

    if row is None:
        raise ValueError(f"raw event not found for trace_id={trace_id}")

    if row[0] == "processed":
        conn.commit()
        return False

    cursor.execute(
        """
        update ops.raw_events
        set
            status = 'processing',
            kafka_topic=%s,kafka_partition=%s,kafka_offset=%s,
            error_message = null
        where trace_id = %s
        """,
        (topic, partition, offset, trace_id),
    )

    cursor.execute(
        """
        insert into ops.pipeline_events
        (
            trace_id,
            pipeline_name,
            stage,
            status,
            message,
            metadata
        )
        values
        (
            %s,
            %s,
            'KAFKA_CONSUME',
            'success',
            %s,
            %s::jsonb
        )
        """,
        (
            trace_id,
            pipeline_name,
            f"Kafka message consumed for orderid={orderid}",
            json.dumps({"topic": topic, "partition": partition, "offset": offset}),
        ),
    )

    cursor.execute(
        """
        insert into ops.pipeline_events
        (
            trace_id,
            pipeline_name,
            stage,
            status,
            message
        )
        values
        (
            %s,
            %s,
            'CLICKHOUSE_WRITE',
            'running',
            %s
        )
        """,
        (trace_id, pipeline_name, f"rebuilding fact orderid={orderid}"),
    )

    conn.commit()

    return True


def mark_processed(cursor, conn, pipeline_name, trace_id, orderid):
    cursor.execute(
        """
        update ops.raw_events
        set
            status = 'processed',
            processed_at = current_timestamp,
            error_message = null
        where trace_id = %s
        """,
        (trace_id,),
    )

    cursor.execute(
        """
        insert into ops.pipeline_events
        (
            trace_id,
            pipeline_name,
            stage,
            status,
            message
        )
        values
        (
            %s,
            %s,
            'CLICKHOUSE_WRITE',
            'success',
            %s
        )
        """,
        (trace_id, pipeline_name, f"{pipeline_name} processed event"),
    )

    conn.commit()


def mark_processing_failed(
    cursor, conn, pipeline_name, trace_id, orderid, error_message
):
    cursor.execute(
        """
        update ops.raw_events
        set
            status = 'failed',
            retry_count = retry_count + 1,
            error_message = %s
        where trace_id = %s
        """,
        (error_message, trace_id),
    )

    cursor.execute(
        """
        insert into ops.pipeline_events
        (
            trace_id,
            pipeline_name,
            stage,
            status,
            message
        )
        values
        (
            %s,
            %s,
            'CLICKHOUSE_WRITE',
            'failed',
            %s
        )
        """,
        (trace_id, pipeline_name, error_message),
    )

    conn.commit()
