from dataclasses import dataclass
from typing import Literal


@dataclass(kw_only=True, frozen=True)
class QueueOptions:
    name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
