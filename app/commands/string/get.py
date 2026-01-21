from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext


@registry.register(b"GET")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return args[0]

    def execute(self, parsed: bytes, context: InnerContext) -> object:
        value = context.store.get(parsed)

        if value is not None:
            response = f"${len(value)}\r\n{value.decode('utf-8')}\r\n"
        else:
            response = "$-1\r\n"

        context.connection.sendall(response.encode())
        return None
