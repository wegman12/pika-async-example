from dataclasses import dataclass, field
from typing import Any, Coroutine

from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustConnection

from app import configuration


@dataclass(kw_only=True, frozen=True)
class ConnectionOptions:
    host: str = field(default_factory=lambda: configuration.DEFAULT_CONNECTION_HOST)
    port: int = field(default_factory=lambda: configuration.DEFAULT_CONNECTION_PORT)
    username: str = field(default_factory=lambda: configuration.DEFAULT_CONNECTION_USER)
    password: str = field(
        default_factory=lambda: configuration.DEFAULT_CONNECTION_PASSWORD
    )

    def generate_connection(self) -> Coroutine[Any, Any, AbstractRobustConnection]:
        return connect_robust(
            url=None,
            host=self.host,
            port=self.port,
            login=self.username,
            password=self.password,
        )
