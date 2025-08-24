import logging
import multiprocessing
import multiprocessing.pool

from typing import Callable

from app import configuration as app_configuration
from app import models
from app.models.stop_flag import StopFlag

from ._worker_thread import WorkerThread


class WorkerThreadManager:
    def __init__(
        self,
        *,
        worker_count: int = app_configuration.DEFAULT_WORKER_INSTANCES,
    ) -> None:
        self._flag = StopFlag()

        self._worker_count = worker_count
        self._pool: multiprocessing.pool.Pool | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def is_running(self) -> bool:
        return self._pool is not None

    def start_workers(
        self,
        queue_options: models.QueueOptions,
        connection_options: models.ConnectionOptions | None = None,
        channel_options: models.ChannelOptions | None = None,
    ) -> None:
        if self.is_running:
            self._logger.warning("Workers are already running.")
            return

        def factory() -> WorkerThread:
            return WorkerThread(
                queue_options=queue_options,
                connection_options=connection_options or models.ConnectionOptions(),
                channel_options=channel_options or models.ChannelOptions(),
                flag=self._flag,
            )

        self._flag.stop = False
        self._pool = multiprocessing.Pool(processes=self._worker_count)
        for i in range(0, self._worker_count):
            self._logger.info(f"Starting worker #{i + 1}...")
            thread = factory()
            self._pool.apply_async(
                WorkerThread.consume_sync,
                args=(thread,),
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
