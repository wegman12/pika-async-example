import logging
import multiprocessing
import multiprocessing.pool

from typing import Callable

from app import configuration as app_configuration
from app import models
from app.models.stop_flag import StopFlag

from ._worker_thread import WorkerThread


class WorkerThreadManager:
    @staticmethod
    def create_worker_thread_factory(
        *,
        queue_options: models.QueueOptions,
        connection_options: models.ConnectionOptions = models.ConnectionOptions(),
        channel_options: models.ChannelOptions = models.ChannelOptions(),
        flag: StopFlag,
    ) -> Callable[[], WorkerThread]:
        def factory() -> WorkerThread:
            return WorkerThread(
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
        self._thread_factory = WorkerThreadManager.create_worker_thread_factory(
            queue_options=queue_options,
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
