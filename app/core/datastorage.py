import time

from dataclasses import dataclass, field


@dataclass
class DataStorage:
    data: dict[bytes, bytes] = field(default_factory=dict)
    expiry: dict[bytes, int] = field(default_factory=dict)

    def set(self, key: bytes, value: bytes, expiry_ms: int | None = None) -> None:
        self.data[key] = value

        if expiry_ms:
            self.expiry[key] = expiry_ms
        elif key in self.expiry:
            del self.expiry[key]


    def set_with_ttl(self, key: bytes, value: bytes, ttl_ms: int | None = None) -> None:
        if ttl_ms is not None:
            expiry_ms = int(time.time() * 1000) + ttl_ms
            self.set(key, value, expiry_ms)
        else:
            self.set(key, value)


    def get(self, key: bytes) -> bytes | None:
        if key in self.expiry:
            current_time_ms = int(time.time() * 1000)

            if current_time_ms >= self.expiry[key]:
                del self.data[key]
                del self.expiry[key]

        return self.data.get(key)


    def keys(self, pattern: bytes) -> list[bytes]:
        current_time_ms = int(time.time() * 1000)

        expired_keys = [
            key for key in self.data.keys()
            if key in self.expiry and current_time_ms >= self.expiry[key]
        ]

        for key in expired_keys:
            del self.data[key]
            del self.expiry[key]

        if pattern == b"*":
            return list(self.data.keys())

        return []
