from dataclasses import dataclass, field

from app import configuration


@dataclass(kw_only=True, frozen=True)
class ChannelOptions:
    prefetch_count: int = field(
        default_factory=lambda: configuration.DEFAULT_CONSUMER_PREFETCH_COUNT
    )
