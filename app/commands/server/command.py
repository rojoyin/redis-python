from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import Array, RespValue, SimpleString


@registry.register(b"COMMAND")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return None

    def execute(self, parsed: object, context: InnerContext) -> RespValue:
        result_string = ""
        result = [SimpleString(result_string)]
        return Array(result)
