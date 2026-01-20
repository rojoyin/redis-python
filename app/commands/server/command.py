from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext


@registry.register(b"COMMAND")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return None

    def execute(self, parsed: object, context: InnerContext) -> object:
        context.connection.sendall(b"*0\r\n")
        return None
