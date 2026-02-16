from dataclasses import dataclass

from app.commands.contract import CommandHandler
from app.core.innercontext import InnerContext
from app.protocol.types import RespValue, BulkString
from app.commands.registry import registry


@dataclass
class InfoPayload:
    subcommand: bytes | None = None


@registry.register(b"INFO")
class Handler(CommandHandler):

    def parse(self, args: list[bytes]) -> object:
        return InfoPayload(subcommand=args[0]) if args else InfoPayload()

    def execute(self, parsed: InfoPayload, context: InnerContext) -> RespValue:
        configs = []

        replication_configs = [b"role:master"]

        configs.append(b"\n".join(replication_configs))

        if parsed.subcommand == b"replication":
            return BulkString(b"\n".join(replication_configs))

        return BulkString(b"\n".join(configs))
