from dataclasses import dataclass
from app import configuration


@dataclass(kw_only=True, frozen=True)
class ChannelOptions:
    prefetch_count: int = configuration.DEFAULT_CONSUMER_PREFETCH_COUNT
