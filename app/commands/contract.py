from abc import ABC, abstractmethod

from app.protocol.types import RespValue


class CommandHandler(ABC):

    @abstractmethod
    def parse(self, args: list[bytes]) -> object: ...

    @abstractmethod
    def execute(self, parsed: object, context: "InnerContext") -> RespValue: ...

    def __call__(self, args: list[bytes], context: "InnerContext") -> RespValue:
        parsed = self.parse(args)
        return self.execute(parsed, context)
