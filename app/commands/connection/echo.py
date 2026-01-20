from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext


@registry.register(b"ECHO")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return args[1:]

    def execute(self, parsed: object, context: InnerContext) -> object:
        response = f"${len(parsed[0])}\r\n{parsed[0].decode('utf-8')}\r\n"
        context.connection.sendall(response.encode())
        return None
