import threading

from dataclasses import dataclass, field


@dataclass
class DataStorage:
    data: dict[bytes, bytes] = field(default_factory=dict)

    def set(self, key: bytes, value: bytes, ttl_seconds: float | None = None) -> None:
        if ttl_seconds is not None:
            threading.Timer(ttl_seconds, self.data.pop, args=[key]).start()

        self.data[key] = value

    def get(self, key: bytes) -> bytes | None:
        return self.data.get(key)

    def delete_key(self, key: bytes) -> None:
        del self.data[key]

    def keys(self, pattern: bytes) -> list[bytes]:
        if pattern == b"*":
            return list(self.data.keys())

        return []
