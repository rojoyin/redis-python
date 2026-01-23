from app.core.datastorage import DataStorage


class ConfigStorage(DataStorage):

    def set(self, key: bytes, value: bytes, ttl_milliseconds: int | None = None) -> None:
        self.data[key] = value
