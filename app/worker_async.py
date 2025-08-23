#!/usr/bin/env python

"""
Create multiple RabbitMQ connections from a single thread, using Pika and multiprocessing.Pool.
Based on tutorial 2 (http://www.rabbitmq.com/tutorials/tutorial-two-python.html).
"""

import multiprocessing
import time

import pika
import pika.channel
import pika.adapters
import pika.adapters.blocking_connection
import pika.adapters.asyncio_connection
from pika.spec import Basic
from pika.spec import BasicProperties
from app.stop_flag import StopFlag


def callback(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    method: Basic.Deliver,
    _: BasicProperties,
    body: bytes,
) -> None:
    print(
        " [x] %r received %r"
        % (
            multiprocessing.current_process(),
            body.decode(),
        )
    )
    time.sleep(body.decode().count("."))
    # print " [x] Done"
    ch.basic_ack(delivery_tag=method.delivery_tag or 0)


def consume() -> None:
    connection = pika.adapters.asyncio_connection.AsyncioConnection(
        pika.ConnectionParameters("localhost")
    )
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    channel.queue_declare(queue="hello", durable=True)

    channel.basic_consume(queue="hello", on_message_callback=callback)

    print(
        f" [*] Waiting for messages on {repr(multiprocessing.current_process())}. To exit press CTRL+C"
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass


def start_workers(flag: StopFlag) -> None:
    workers = 5
    pool = multiprocessing.Pool(processes=workers)
    for i in range(0, workers):
        print(" [*] Starting worker %d" % i)
        pool.apply_async(consume)

    while not flag.stop:
        continue
    print(" [*] Exiting...")
    pool.terminate()
    pool.join()
