from dataclasses import dataclass
import asyncio
import multiprocessing
import multiprocessing.pool
from typing import Callable

from aio_pika.abc import AbstractIncomingMessage
from aio_pika import connect_robust

from app.stop_flag import StopFlag
from app import models
from app import configuration as app_configuration
import logging
import logging.config
from definitions import ROOT_DIR
import os
import logging.config
import yaml


class WorkerAsync:
    @dataclass(kw_only=True, frozen=True)
    class _WorkerAsyncThread:
        queue_options: models.QueueOptions
        connection_options: models.ConnectionOptions
        channel_options: models.ChannelOptions
        flag: StopFlag

        async def _callback(self, message: AbstractIncomingMessage) -> None:
            logger = logging.getLogger(self.__class__.__name__)
            text = message.body.decode()
            logger.info(
                f"{repr(multiprocessing.current_process())} received {repr(text)}"
            )
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

    @staticmethod
    def create_worker_thread_factory(
        *,
        queue_options: models.QueueOptions,
        worker_count: int = app_configuration.DEFAULT_WORKER_INSTANCES,
        connection_options: models.ConnectionOptions = models.ConnectionOptions(),
        channel_options: models.ChannelOptions = models.ChannelOptions(),
        flag: StopFlag,
    ) -> Callable[[], _WorkerAsyncThread]:
        def factory() -> WorkerAsync._WorkerAsyncThread:
            return WorkerAsync._WorkerAsyncThread(
                queue_options=queue_options,
                connection_options=connection_options,
                channel_options=channel_options,
                flag=flag,
            )

        return factory

    def __init__(
        self,
        *,
        queue_options: models.QueueOptions,
        worker_count: int = app_configuration.DEFAULT_WORKER_INSTANCES,
        connection_options: models.ConnectionOptions = models.ConnectionOptions(),
        channel_options: models.ChannelOptions = models.ChannelOptions(),
    ) -> None:
        self._flag = StopFlag()
        self._thread_factory = WorkerAsync.create_worker_thread_factory(
            queue_options=queue_options,
            worker_count=worker_count,
            connection_options=connection_options,
            channel_options=channel_options,
            flag=self._flag,
        )

        self._worker_count = worker_count
        self._pool: multiprocessing.pool.Pool | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def is_running(self) -> bool:
        return self._pool is not None

    def start_workers(
        self,
    ) -> None:
        if self.is_running:
            self._logger.warning("Workers are already running.")
            return
        self._pool = multiprocessing.Pool(processes=self._worker_count)
        for i in range(0, self._worker_count):
            thread = self._thread_factory()
            self._pool.apply_async(
                thread.consume_sync,
                error_callback=lambda e: self._logger.error(e),
            )

    def stop_workers(self) -> None:
        if not self.is_running or self._pool is None:
            self._logger.warning("Workers are not running.")
            return
        try:
            self._flag.stop = True
            self._pool.terminate()
            self._pool.join()
        finally:
            self._pool = None
