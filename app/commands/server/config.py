from app.commands.registry import registry
from app.commands.contract import CommandHandler
from app.core.innercontext import InnerContext
from app.protocol.types import BulkString, Array


@registry.register(B"CONFIG")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return args

    def execute(self, parsed: object, context: InnerContext) -> object:

        if parsed[0] == B"GET":
            parameter_name = parsed[1]
            config_parameter = context.config_store.get(parameter_name)
            result = Array(
                [
                    BulkString(parameter_name),
                    BulkString(config_parameter)
                ]
            )
            return result
