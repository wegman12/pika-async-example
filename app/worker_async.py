import asyncio
import multiprocessing

from aio_pika.abc import AbstractIncomingMessage
from aio_pika import connect_robust

from app.stop_flag import StopFlag


async def callback(message: AbstractIncomingMessage) -> None:
    text = message.body.decode()
    print(f" [x] {repr(multiprocessing.current_process())} received {repr(text)}")
    await asyncio.sleep(text.count("."))
    await message.ack()


async def consume() -> None:
    connection = await connect_robust("amqp://guest:guest@localhost/")
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
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass


async def run_until_cancellation(flag: StopFlag) -> None:
    try:
        task = asyncio.create_task(consume())
        while not flag.stop and not task.done():
            await asyncio.sleep(0.01)
        if not task.done():
            task.cancel()
        await task
    except (asyncio.CancelledError, KeyboardInterrupt):
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
