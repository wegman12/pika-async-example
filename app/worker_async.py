#!/usr/bin/env python

"""
Create multiple RabbitMQ connections from a single thread, using Pika and multiprocessing.Pool.
Based on tutorial 2 (http://www.rabbitmq.com/tutorials/tutorial-two-python.html).
"""

import asyncio
import multiprocessing
import time

from aio_pika.abc import AbstractIncomingMessage
from aio_pika.connection import connect

from app.stop_flag import StopFlag


async def callback(message: AbstractIncomingMessage) -> None:
    text = message.body.decode()
    print(f" [x] {repr(multiprocessing.current_process())} received {repr(text)}")
    await asyncio.sleep(text.count("."))
    await message.ack()


async def consume() -> None:
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)  # Declaring queue
        queue = await channel.declare_queue(
            "hello",
            durable=True,
        )

        await queue.consume(callback)

        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass


async def run_until_cancellation(flag: StopFlag) -> None:
    task = asyncio.create_task(consume())
    while not flag.stop and not task.done():
        pass
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def consume_sync(flag: StopFlag) -> None:
    asyncio.run(run_until_cancellation(flag))


def start_workers(flag: StopFlag) -> None:
    workers = 5
    pool = multiprocessing.Pool(processes=workers)
    for i in range(0, workers):
        pool.apply_async(consume_sync, args=(flag,))
    try:
        while not flag.stop:
            continue
    except KeyboardInterrupt:
        pass
    finally:
        pool.terminate()
        pool.join()
