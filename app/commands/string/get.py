from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import NullBulkString, BulkString


@registry.register(b"GET")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return args[0]

    def execute(self, parsed: bytes, context: InnerContext) -> object:
        value = context.store.get(parsed)

        if value is None:
            return NullBulkString()

        return BulkString(value)
