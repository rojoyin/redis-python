from dataclasses import dataclass, field

ConfigValue = int | float | bool | str | bytes


@dataclass
class ConfigStorage:
    data: dict[str, ConfigValue] = field(default_factory=dict)

    def set(self, key: str, value: ConfigValue) -> None:
        self.data[key] = value

    def get(self, key: str, default: ConfigValue | None = None) -> ConfigValue | None:
        return self.data.get(key, default)
