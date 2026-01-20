from abc import ABC, abstractmethod


class CommandHandler(ABC):

    @abstractmethod
    def parse(self, args: list[bytes]) -> object: ...

    @abstractmethod
    def execute(self, parsed: object, context: "InnerContext") -> object: ...

    def __call__(self, args: list[bytes], context: "InnerContext") -> object:
        parsed = self.parse(args)
        return self.execute(parsed, context)
