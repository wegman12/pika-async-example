from dataclasses import dataclass
from typing import Any, Coroutine

from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustConnection

from app import configuration


@dataclass(kw_only=True, frozen=True)
class ConnectionOptions:
    host: str = configuration.DEFAULT_CONNECTION_HOST
    port: int = configuration.DEFAULT_CONNECTION_PORT
    username: str = configuration.DEFAULT_CONNECTION_USER
    password: str = configuration.DEFAULT_CONNECTION_PASSWORD

    def generate_connection(self) -> Coroutine[Any, Any, AbstractRobustConnection]:
        return connect_robust(
            url=None,
            host=self.host,
            port=self.port,
            login=self.username,
            password=self.password,
        )
