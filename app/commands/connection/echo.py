from dataclasses import dataclass

from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import BulkString, RespValue


@dataclass
class EchoMessage:
    received: bytes

@registry.register(b"ECHO")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> EchoMessage:
        return EchoMessage(args[0])

    def execute(self, parsed: EchoMessage, context: InnerContext) -> RespValue:
        return BulkString(parsed.received)
