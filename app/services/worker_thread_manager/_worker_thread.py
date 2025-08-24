from dataclasses import dataclass
import asyncio
import multiprocessing
import multiprocessing.pool

from aio_pika.abc import AbstractIncomingMessage
from aio_pika import connect_robust

from app.models.stop_flag import StopFlag
from app import models
import logging
import logging.config
from definitions import ROOT_DIR
import os
import logging.config
import yaml


@dataclass(kw_only=True, frozen=True)
class WorkerThread:
    queue_options: models.QueueOptions
    connection_options: models.ConnectionOptions
    channel_options: models.ChannelOptions
    flag: StopFlag

    async def _callback(self, message: AbstractIncomingMessage) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        text = message.body.decode()
        logger.info(f"{repr(multiprocessing.current_process())} received {repr(text)}")
        await asyncio.sleep(text.count("."))
        await message.ack()

    async def _consume(self) -> None:
        connection = await connect_robust(
            url=None,
            host=self.connection_options.host,
            port=self.connection_options.port,
            login=self.connection_options.username,
            password=self.connection_options.password,
        )
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(
                prefetch_count=self.channel_options.prefetch_count
            )  # Declaring queue
            queue = await channel.declare_queue(
                name=self.queue_options.name,
                durable=self.queue_options.durable,
                exclusive=self.queue_options.exclusive,
                auto_delete=self.queue_options.auto_delete,
            )

            await queue.consume(self._callback)

            try:
                await asyncio.Future()
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass

    async def _run_until_cancellation(
        self,
    ) -> None:
        try:
            task = asyncio.create_task(self._consume())
            while not self.flag.stop and not task.done():
                await asyncio.sleep(0.01)
            if not task.done():
                task.cancel()
            await task
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass

    def consume_sync(
        self,
    ) -> None:
        with open(os.path.join(ROOT_DIR, "log_config.yaml"), "r") as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)

        logging.config.dictConfig(config)
        try:
            asyncio.run(self._run_until_cancellation())
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
