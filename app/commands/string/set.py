from dataclasses import dataclass

from app.commands.contract import CommandHandler
from app.commands.registry import registry
from app.core.innercontext import InnerContext
from app.protocol.types import SimpleString, Error, RespValue

MS_FLAG= b"px"
SEC_FLAG= b"ex"


@dataclass
class VariableMetadata:
    var_name: bytes
    var_value: bytes
    ttl_seconds: float | None = None


@registry.register(b"SET")
class Handler(CommandHandler):
    def parse(self, args: list[bytes]) -> VariableMetadata | Error:
        if len(args) < 2:
            return Error("Incomplete command")

        parsed_var_data = VariableMetadata(
            var_name=args[0],
            var_value=args[1]
        )

        if len(args) == 2:
            return parsed_var_data

        extra_args = [v.lower() for v in args[2:]]

        if MS_FLAG in extra_args:
            ttl_flag_index = extra_args.index(MS_FLAG)
            ttl_value_index = ttl_flag_index + 1

            if ttl_value_index >= len(extra_args):
                return Error("Missing value for milliseconds")

            parsed_var_data.ttl_seconds = int(extra_args[ttl_value_index]) / 1000.0
        elif SEC_FLAG in extra_args:
            ttl_flag_index = extra_args.index(SEC_FLAG)
            ttl_value_index = ttl_flag_index + 1

            if ttl_value_index >= len(extra_args):
                return Error("Missing value for seconds")

            parsed_var_data.ttl_seconds = int(extra_args[ttl_value_index])

        return parsed_var_data

    def execute(self, parsed: VariableMetadata, context: InnerContext) -> RespValue:
        context.store.set(parsed.var_name, parsed.var_value, parsed.ttl_seconds)
        return SimpleString("OK")
