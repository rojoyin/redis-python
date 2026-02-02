from dataclasses import dataclass, field
from typing import TypeAlias, Union

@dataclass
class SimpleString:
    text: str

@dataclass
class BulkString:
    data: bytes

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
]
