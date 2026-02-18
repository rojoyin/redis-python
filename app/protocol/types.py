from dataclasses import dataclass, field
from typing import TypeAlias, Union


@dataclass(slots=True)
class SimpleString:
    text: str


@dataclass(slots=True)
class BulkString:
    data: bytes

    def __init__(self, data: bytes | str):
        self.data = data.encode("utf-8") if isinstance(data, str) else data


@dataclass(slots=True)
class Integer:
    value: int


@dataclass(slots=True)
class Error:
    message: str


@dataclass(slots=True)
class NullBulkString:
    """Represents RESP Null Bulk String: $-1"""


@dataclass(slots=True)
class Array:
    items: list["RespValue"] = field(default_factory=list)


RespValue: TypeAlias = Union[
    SimpleString,
    BulkString,
    Integer,
    Error,
    NullBulkString,
    Array,
    None,
]
