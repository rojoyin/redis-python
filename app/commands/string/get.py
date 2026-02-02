from dataclasses import dataclass

from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import NullBulkString, BulkString, RespValue, Error


@dataclass
class GetParameter:
    parameter_name: bytes

@registry.register(b"GET")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> GetParameter | Error:

        if not args:
            return Error("Missing key name")

        return GetParameter(args[0])

    def execute(self, parsed: GetParameter, context: InnerContext) -> RespValue:
        value = context.store.get(parsed.parameter_name)

        if value is None:
            return NullBulkString()

        return BulkString(value)
