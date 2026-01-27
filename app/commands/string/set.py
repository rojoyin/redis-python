from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import SimpleString


@registry.register(b"SET")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> object:
        var_name, var_value = args[0], args[1]
        parsed_command = [v.lower() for v in args]
        parse_as_milliseconds = False
        ttl_value = None

        if len(parsed_command) > 2:
            if b"px" in parsed_command:
                ttl_flag_index = parsed_command.index(b"px")
                parse_as_milliseconds = True
            elif b"ex" in parsed_command:
                ttl_flag_index = parsed_command.index(b"ex")
            else:
                raise Exception(f"Unsupported option for the SET command")

            val = int(parsed_command[ttl_flag_index + 1])
            ttl_value = val if not parse_as_milliseconds else val / 1000

        return var_name, var_value, ttl_value

    def execute(self, parsed: object, context: InnerContext) -> object:
        var_name, var_value, ttl_value = parsed
        context.store.set(var_name, var_value, ttl_value)
        return SimpleString("OK")
