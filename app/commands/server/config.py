from app.commands.registry import registry
from app.commands.contract import CommandHandler
from app.core.innercontext import InnerContext


@registry.register(B"CONFIG")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        return args

    def execute(self, parsed: object, context: InnerContext) -> object:

        if parsed[0] == B"GET":
            parameter_name = parsed[1]
            config_parameter = context.config_store.get(parameter_name)
            response = f"""*2\r\n${len(parameter_name)}\r\n{parameter_name.decode("utf-8")}\r\n${len(config_parameter)}\r\n{config_parameter}\r\n"""
            return response
