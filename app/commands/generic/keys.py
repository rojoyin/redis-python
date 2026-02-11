from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import RespValue, Array, BulkString


@registry.register(b"KEYS")
class Handler(CommandHandler):
    def __init__(self):
        self.store = {}

    def parse(self, args: list[bytes]) -> bytes:
        pattern = args[0]
        return pattern

    def execute(self, parsed: bytes, context: InnerContext) -> RespValue:
        pattern = parsed
        key_names = context.store.keys(pattern)
        keys = [BulkString(key_name) for key_name in key_names]
        return Array(keys)
