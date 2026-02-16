from abc import ABC, abstractmethod

from app.core.innercontext import InnerContext
from app.protocol.types import RespValue, Error


class CommandHandler(ABC):

    @abstractmethod
    def parse(self, args: list[bytes]) -> object: ...

    @abstractmethod
    def execute(self, parsed: object, context: InnerContext) -> RespValue: ...

    def __call__(self, args: list[bytes], context: InnerContext) -> RespValue:
        parsed = self.parse(args)

        if isinstance(parsed, Error):
            return parsed

        return self.execute(parsed, context)
