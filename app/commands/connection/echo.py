from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import BulkString, RespValue


@registry.register(b"ECHO")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return args

    def execute(self, parsed: object, context: InnerContext) -> RespValue:
        return BulkString(parsed[0])
