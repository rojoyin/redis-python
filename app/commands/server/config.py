from dataclasses import dataclass

from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import BulkString, Array, RespValue, Error


@dataclass
class ConfigPayload:
    subcommand: bytes
    parameter_name: str | None = None

@registry.register(B"CONFIG")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> ConfigPayload | Error:
        if not args or len(args) < 2:
            return Error("Command syntax error")

        return ConfigPayload(subcommand=args[0],parameter_name=args[1].decode("utf-8"))

    def execute(self, parsed: ConfigPayload, context: InnerContext) -> RespValue:

        if parsed.subcommand.lower() == B"get":
            config_parameter = context.config_store.get(parsed.parameter_name)

            if not config_parameter:
                return Array()

            result = Array(
                [
                    BulkString(parsed.parameter_name),
                    BulkString(config_parameter)
                ]
            )
            return result
