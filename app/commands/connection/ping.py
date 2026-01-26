from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import SimpleString, RespValue


@registry.register(b"PING")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return None

    def execute(self, parsed: object, context: InnerContext) -> RespValue:
        return SimpleString("PONG")
