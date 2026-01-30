from dataclasses import dataclass

from app.commands.registry import registry
from app.commands.contract import CommandHandler
from app.core.innercontext import InnerContext
from app.protocol.types import BulkString, Array


@dataclass
class ConfigPayload:
    subcommand: bytes
    parameter_name: bytes | None = None

@registry.register(B"CONFIG")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> ConfigPayload:
        return ConfigPayload(subcommand=args[0],parameter_name=args[1])

    def execute(self, parsed: ConfigPayload, context: InnerContext) -> object:

        if parsed.subcommand == B"GET":
            config_parameter = context.config_store.get(parsed.parameter_name)
            result = Array(
                [
                    BulkString(parsed.parameter_name),
                    BulkString(config_parameter)
                ]
            )
            return result
