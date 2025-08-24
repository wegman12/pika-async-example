from dataclasses import dataclass
from app import configuration
from typing import Literal, Any


@dataclass(kw_only=True, frozen=True)
class ConnectionOptions:
    host: str = configuration.DEFAULT_CONNECTION_HOST
    port: int = configuration.DEFAULT_CONNECTION_PORT
    username: str = configuration.DEFAULT_CONNECTION_USER
    password: str = configuration.DEFAULT_CONNECTION_PASSWORD
