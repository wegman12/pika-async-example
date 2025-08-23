#!/usr/bin/env python
import sys

import pika


def create_task(message: str | None = None) -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="hello", durable=True)
    channel.basic_qos(prefetch_count=1)

    message = message or "Hello World!"

    channel.basic_publish(
        exchange="",
        routing_key="hello",
        body=message.encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ),
    )

    connection.close()
