import threading

from dataclasses import dataclass, field


@dataclass
class DataStorage:
    data: dict[bytes, bytes] = field(default_factory=dict)

    def set(self, key: bytes, value: bytes, ttl_milliseconds: int | None = None) -> None:
        if ttl_milliseconds is not None:
            threading.Timer(ttl_milliseconds, self.data.pop, args=[key]).start()

        self.data[key] = value

    def get(self, key: bytes) -> bytes | None:
        return self.data.get(key)

    def delete_key(self, key: bytes) -> None:
        del self.data[key]
