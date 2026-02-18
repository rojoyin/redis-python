from dataclasses import dataclass, field
from typing import TypeAlias, Union


@dataclass
class SimpleString:
    text: str


@dataclass
class BulkString:
    data: bytes

    def __init__(self, data: bytes | str):
        if isinstance(data, str):
            self.data = data.encode("utf-8")
        else:
            self.data = data


@dataclass
class Integer:
    value: int


@dataclass
class Error:
    message: str


@dataclass
class NullBulkString:
    """Represents RESP Null Bulk String: $-1"""


@dataclass
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
