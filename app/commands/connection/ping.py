from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext


@registry.register(b"PING")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return None

    def execute(self, parsed: object, context: InnerContext) -> object:
        return "+PONG\r\n"
