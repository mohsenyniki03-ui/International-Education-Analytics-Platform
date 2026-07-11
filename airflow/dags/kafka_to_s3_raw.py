from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(schedule="@hourly", start_date=datetime(2026, 7, 11))
def kafka_to_s3_raw():
    
    @task
    def consume_kafka() -> list:
        # read events from kafka topics
        # return them as a list

        from confluent_kafka import Consumer, KafkaError
        import json
        import os

        # Kafka consumer configuration
        consumer = Consumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'group.id': 'airflow-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })  

        # Subscribe to the Kafka topic
        topics = [
        "intl.student.applications",
        "intl.student.documents",
        "intl.student.visa_status",
        "intl.student.enrollment",
        "intl.student.registration",
        "intl.student.opt_cpt",
        "intl.student.status_change",
        "intl.student.graduation",
        ]

        consumer.subscribe(topics)
        events = []
        empty_polls = 0
        max_empty_polls = 5

        while empty_polls < max_empty_polls:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                continue
            events.append(json.loads(msg.value().decode("utf-8")))
            empty_polls = 0

        consumer.close()
        
        return events


    @task
    def upload_to_s3(events: list):
        # convert the list of events to a parquet file
        # upload the parquet file to s3
        # conceptually this task needs to do the following:
        # 1. group events by topic
        # 2. convert each converted group to a parquet file
        # 3. upload these parquet files to s3 with the following path: s3://<bucket_name>/raw/<topic_name>/<date>/<time>.parquet

        import os
        import json
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from io import BytesIO
        from datetime import datetime, timezone
        from collections import defaultdict

        # now we need to group the events by topic
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name="us-east-1",
        )

        bucket = "eduflow-raw"

        # create the bucket if it doesn't exist
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)

        # group events by topic
        grouped = defaultdict(list)
        for event in events:
            topic = event.get("event_type")
            grouped[topic].append(event)

        # for each group, convert to parquet and upload to s3
        date = datetime.now(timezone.utc)
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")

        for topic, topic_events in grouped.items():
            df = pd.DataFrame(topic_events)
            
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            key = f"{topic}/year={year}/month={month}/day={day}/events.parquet"
            
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue(),
            )


    events = consume_kafka()
    upload_to_s3(events)

kafka_to_s3_raw_dag = kafka_to_s3_raw()